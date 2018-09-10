namespace ExtendEverything

open System
open System.Threading

module Asynchronous = 
  type AsyncRepository<'a, 'b> = 'a -> Async<'b>

  (* a repository that always fails with the given exception *)
  let failingRepository ex = fun _ -> async { return(raise ex) }

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

  let private batchResultForSingleResults singleResultsAsync = 
    async {
      let! singleResults = singleResultsAsync

      let result = 
        {
          found = 
            singleResults
            |> Seq.choose(fun res -> match res with | Found(a, b) -> Some((a, b)) | _ -> None)
          notFound = singleResults |> Seq.choose(fun res -> match res with | NotFound(a) -> Some(a) | _ -> None)
          failed = singleResults |> Seq.choose(fun res -> match res with | Failed(a, ex) -> Some((a, ex)) | _ -> None)
        }

      return result
    }

  type BatchQueryRepository<'a, 'b> = seq<'a> -> Async<BatchQueryResult<'a, 'b>>

  let batchQueryRepository r = 
    let getResultForKey aKey = 
      async {
        try
          let! result = r(aKey)
          match result with
          | Some(bVal) -> return Found(aKey, bVal)
          | None -> return NotFound(aKey)
        with
        | ex -> return Failed(aKey, ex)
      }

    fun (aKeys: seq<'a>) -> 
      aKeys
      |> Seq.map getResultForKey
      |> Async.Parallel
      |> batchResultForSingleResults

  (* a repository that can delegate to one of two repositories, based on the result of a function applied with the input key *)
  let switchingRepository<'a, 'b>(f: 'a -> bool, repoA: AsyncRepository<'a, 'b>, repoB: AsyncRepository<'a, 'b>): AsyncRepository<'a, 'b> = 
    fun (a: 'a) -> if f(a) then repoA(a) else repoB(a)

  (* a repository that delegates to one of many repositories, based on the input key *)
  let mappingRepository<'a, 'b>(f: 'a -> AsyncRepository<'a, 'b>): AsyncRepository<'a, 'b> = 
    fun (a: 'a) -> f(a)(a)

  (* a repository that prefers consulting its synchronous cache first, before querying a repository *)
  let cachingRepository(c: ExtendEverything.Cache.Synchronous.ICache<'a, 'b>, r: AsyncRepository<'a, 'b>): AsyncRepository<'a, 'b> =
    fun (a: 'a) -> 
      async {
        let cacheQueryResult = c.Get(a)
        match cacheQueryResult with
        | Some(result) -> return(result)
        | _ -> 
          let! result = r(a)
          c.Set(a)(result)
          return(result)
      }

  (* a repository that prefers consulting its asynchronous cache first, before querying a repository *)
  let asyncCachingRepository(c: ExtendEverything.Cache.Asynchronous.IAsyncCache<'a, 'b>, r: AsyncRepository<'a, 'b>): AsyncRepository<'a, 'b> =
    fun (a: 'a) -> 
      async {
        let! cacheQueryResult = c.Get(a)
        match cacheQueryResult with
        | Some(result) -> return(result)
        | _ -> 
          let! result = r(a)
          Async.Start(c.Set(a)(result))
          return(result)
      }

  (* a repository that optionally retries on exceptions, up to a limit *)
  let retryingRepository(r: AsyncRepository<'a, 'b>, retries: int, retryException: exn -> bool, retryDelayInMs: int -> int) = 
    let rec tryFn remaining aKey =
      async {
        let! resultChoice = Async.Catch<'b>(r(aKey))
        match resultChoice with
        | Choice1Of2 res -> return res
        | Choice2Of2 ex when remaining > 0 && (retryException ex) ->
          let retryDelay = retryDelayInMs(retries - remaining)
          let! _ =
            if (retryDelay <> 0) then
              Async.Sleep(retryDelay)
            else
              async { return () }

          return! tryFn (remaining - 1) aKey
        | Choice2Of2 ex -> return raise ex
      }
    
    tryFn retries

  (* a repository that performs a side-effecting operation on a successful query *)
  let sideEffectOnSuccess(r: AsyncRepository<'a, 'b>, effect: 'a * 'b -> Async<unit>) = 
    fun aKey -> 
      async {
        let! result = r(aKey)
        Async.Start(effect(aKey, result))
        return result
      }

  (* a repository that performs a side-effecting operation on a failure *)
  let sideEffectOnFailure(r: AsyncRepository<'a, 'b>, effect: 'a * System.Exception -> Async<unit>) = 
    fun aKey ->
      async {
        let! resultChoice = Async.Catch<'b>(r(aKey))
        match resultChoice with
        | Choice1Of2 res -> return res
        | Choice2Of2 ex ->
          Async.Start(effect(aKey, ex))
          return raise ex
      }

  (* A repository that times each operation, and reports it via the supplied function. *)
  let timingRepository(r: AsyncRepository<'a, 'b>, getTime: unit -> DateTime, reportTime: TimeSpan -> unit) = 
    fun aKey -> 
      async {
        let startTime = getTime()
        try
          return! r(aKey)
        finally
          let endTime = getTime()
          reportTime(endTime - startTime) 
      }

  (* A repository that generates a delay before running a query, using a provided function *)
  let delayed(r: AsyncRepository<'a, 'b>, delay: 'a -> TimeSpan) = 
    fun aKey -> async { 
      let! _ = Async.Sleep(delay(aKey).Milliseconds)
      return r(aKey)
    }

  (* A repository that falls back to another one, in the case of an exception *)
  let repositoryWithFallback(r: AsyncRepository<'a, 'b>, fb: AsyncRepository<'a, 'b>) = 
    fun aKey -> 
      async {
        try
          return! r(aKey)
        with
        | _ -> return! fb(aKey)
      }

  (* A repository that uses a cancellation token to cancel a repository operation after
     the specified timeout. *)
  let timeoutRepository(r: AsyncRepository<'a * CancellationToken, 'b>, ts: 'a -> TimeSpan) = 
    fun aKey -> async {
      use cancellationTokenSource = new CancellationTokenSource(ts(aKey))
      let! result = r(aKey, cancellationTokenSource.Token)
      return result
    }

  (* A repository that implements the circuit-breaker pattern. If the percentage
    of successes gets too low within a specified time-window, queries will be
    serviced from an alternate repository instead.
  *)
  let timeWindowCircuitBreaking
    (
      r: AsyncRepository<'a, 'b>, 
      alternate: AsyncRepository<'a, 'b>, 
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
      let filteredHistory = history |> List.filter(fun (_, timestamp) -> timestamp > (now - timeWindow))
      history <- filteredHistory |> List.take(Math.Min(bufferSize, filteredHistory |> List.length))
      let currentSr = successRate(history)

      if currentSr >= threshold then
        async {
          try
            let! result = r(aKey)

            // add success
            history <- (true, now) :: history
            return result
          with
          | ex -> 
            // add failure, and reraise exception
            history <- (false, now) :: history
            return(raise ex)
        }
      else
        alternate(aKey)

  exception CircuitBrokenException
  (* Circuit-breaker repository that raises a known exception when the
     circuit is broken. *)
  let timeWindowCircuitBreakingFailure
    (
      r: AsyncRepository<'a, 'b>, 
      bufferSize: int, 
      timeWindow: TimeSpan, 
      threshold: double,
      getTime: unit -> DateTime
    ) =
    timeWindowCircuitBreaking(
      r, 
      (fun _ -> async { return(raise CircuitBrokenException) }),
      bufferSize,
      timeWindow,
      threshold,
      getTime
    )

  exception RateLimitException of string

  (* A repository that rate-limits access to it, by a simple time-based rolling
     window. A `RateLimitException` will be thrown if the limit is exceeded. *)
  let windowedRateLimitedRepository(r: AsyncRepository<'a, 'b>, count: int, timeWindow: TimeSpan, getTime: unit -> DateTime) = 
    let mutable history = []
    fun (aKey: 'a) ->
      async {
        let now = getTime()
        let minTimestamp = now - timeWindow
        history <- now :: (history |> List.filter(fun ts -> ts > minTimestamp))

        let shouldRateLimit = (history |> List.length) > count
        if (not shouldRateLimit) then
          return! r(aKey)
        else
          return! raise(RateLimitException("too many attempts within time window"))
      }
