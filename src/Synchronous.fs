namespace ExtendEverything

open System
open System.Threading
open ExtendEverything.Cache.Synchronous

module Synchronous =
  (* basic queryable repository *)
  type Repository<'a, 'b> = 'a -> 'b

  (* a repository that always fails with the given exception *)
  let failingRepository ex = fun _ -> raise ex

  (* Repository type that accepts a seq of keys for its queries, and returns a batch of results for each key *)
  type BatchQueryResult<'a, 'b> = {
    found: seq<'a * 'b>
    notFound: seq<'a>
    failed: seq<'a * System.Exception>
  }

  type private SingleQueryResult<'a, 'b> = 
    | Found of 'a * 'b
    | NotFound of 'a
    | Failed of 'a * System.Exception

  let private batchResultForSingleResults singleResults = 
    {
      found = 
        singleResults
        |> Seq.choose(fun res -> match res with | Found(a, b) -> Some((a, b)) | _ -> None)
      notFound = singleResults |> Seq.choose(fun res -> match res with | NotFound(a) -> Some(a) | _ -> None)
      failed = singleResults |> Seq.choose(fun res -> match res with | Failed(a, ex) -> Some((a, ex)) | _ -> None)
    }

  type BatchQueryRepository<'a, 'b> = seq<'a> -> BatchQueryResult<'a, 'b>
  let batchQueryRepository r = 
    let getResultForKey aKey = 
      try
        match r(aKey) with
        | Some(bVal) -> Found(aKey, bVal)
        | None -> NotFound(aKey)
      with
        | ex -> Failed(aKey, ex)

    fun (aKeys: seq<'a>) -> 
      aKeys
      |> Seq.map getResultForKey
      |> batchResultForSingleResults

  (* a repository that can delegate to one of two repositories, based on the result of a function applied with the input key *)
  let switchingRepository<'a, 'b>(f: 'a -> bool, repoA: Repository<'a, 'b>, repoB: Repository<'a, 'b>): Repository<'a, 'b> = 
    fun (a: 'a) -> if f(a) then repoA(a) else repoB(a)

  (* a repository that delegates to one of many repositories, based on the input key *)
  let mappingRepository<'a, 'b>(f: 'a -> Repository<'a, 'b>): Repository<'a, 'b> = 
    fun (a: 'a) -> f(a)(a)

  (* a repository that prefers consulting its cache first, before querying a repository *)
  let cachingRepository(c: ICache<'a, 'b>, r: Repository<'a, 'b>): Repository<'a, 'b> =
    fun (a: 'a) -> 
      match c.Get(a) with 
      | Some(result) -> result
      | _ -> 
        let result = r(a)
        c.Set(a)(result)
        result

  (* a repository that optionally retries on exceptions, up to a limit *)
  let retryingRepository(r: Repository<'a, 'b>, retries: int, retryException: exn -> bool) = 
    let rec tryFn remaining aKey =
      try
        r(aKey)
      with
      | ex when remaining > 0 && (retryException ex) ->
        tryFn (remaining - 1) aKey
      | _ -> reraise()
    
    tryFn retries

  (* a repository that performs a side-effecting operation on a successful query *)
  let sideEffectOnSuccess(r: Repository<'a, 'b>, effect: 'a * 'b -> unit) = 
    fun aKey -> 
      let result = r(aKey)
      effect(aKey, result) |> ignore
      result

  (* a repository that performs a side-effecting operation on a failure *)
  let sideEffectOnFailure(r: Repository<'a, 'b>, effect: 'a * System.Exception -> unit) = 
    fun aKey ->
      try
        r(aKey)
      with
      | ex -> 
        effect(aKey, ex) |> ignore
        reraise ()

  (* A repository that times each operation, and reports it via the supplied function. *)
  let timingRepository(r: Repository<'a, 'b>, getTime: unit -> DateTime, reportTime: TimeSpan -> unit) = 
    fun aKey -> 
      let startTime = getTime()
      try
        let t = r(aKey)
        t
      finally
        let endTime = getTime()
        reportTime(endTime - startTime)

  (* A repository that falls back to another one, in the case of an exception *)
  let repositoryWithFallback(r: Repository<'a, 'b>, fb: Repository<'a, 'b>) = 
    fun aKey -> 
      try
        r(aKey)
      with
      | _ -> fb(aKey)

  (* A repository that implements the circuit-breaker pattern. If the percentage
    of successes gets too low within a specified time-window, queries will be
    serviced from an alternate repository instead.
  *)
  let timeWindowCircuitBreaking
    (
      r: Repository<'a, 'b>, 
      alternate: Repository<'a, 'b>, 
      bufferSize: int, 
      timeWindow: TimeSpan, 
      threshold: double,
      getTime: unit -> DateTime
    ) =
    let mutable history = []

    let successRate(data: (bool * DateTime) list) = 
      let tabulated = data |> List.countBy(fun (s, _) -> s)
      let successes = 
        tabulated
        |> List.tryFind(fun (s, _) -> s)
        |> Option.map snd
        |> Option.defaultValue(0)

      let failures = 
        tabulated
        |> List.tryFind(fun (s, _) -> not s)
        |> Option.map snd
        |> Option.defaultValue(0)

      let total = successes + failures
      match total with
      | 0 -> 1.0
      | _ -> ((double)successes) / ((double)successes + (double)failures)

    fun aKey ->
      // determine which repository to call
      let now = getTime()
      history <- history |> List.filter(fun (_, timestamp) -> timestamp > (now - timeWindow)) |> List.take(bufferSize)
      let currentSr = successRate(history)

      if currentSr >= threshold then
        try
          let result = r(aKey)

          // add success
          history <- (true, now) :: history
          result
        with
        | _ -> 
          // add failure, and reraise exception
          history <- (false, now) :: history
          reraise ()
      else
        alternate(aKey)

  exception RateLimitException of string

  let windowedRateLimitedRepository(r: Repository<'a, 'b>, count: int, timeWindow: TimeSpan) = 
    let mutable history = []
    fun (aKey: 'a) ->
      let now = DateTime.Now
      let minTimestamp = now - timeWindow
      history <- history |> List.filter(fun ts -> ts > minTimestamp)

      let shouldRateLimit = (history |> List.length) > count
      if (not shouldRateLimit) then
        r(aKey)
      else
        raise(RateLimitException("too many attempts within time window"))
