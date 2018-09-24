module AsynchronousTests

open ExtendEverything.Asynchronous
open System
open System.Threading
open Xunit

[<Fact>]
let ``switching repository uses the A repository when the function returns true`` () =
  let f = fun _ -> true
  let a = fun _ -> async { return "expected" }
  let repo = switchingRepository(f, a, failingRepository(exn "wrong repo"))
  Assert.Equal("expected", Async.RunSynchronously(repo("key")))

[<Fact>]
let ``switching repository uses the B repository when the function returns false`` () = 
  let f = fun _ -> false
  let b = fun _ -> async { return "expected" }
  let repo = switchingRepository(f, failingRepository(exn "wrong repo"), b)
  Assert.Equal("expected", Async.RunSynchronously(repo("key")))

[<Fact>]
let ``mapping repository uses the correct repository for the input key`` () = 
  let f a = 
      match a with
      | "1" -> fun _ -> async { return "a" }
      | "2" -> fun _ -> async { return "b" }
      | _ -> fun _ -> async { return failwith "unexpected" }

  let r = mappingRepository(f)
  Assert.Equal("a", Async.RunSynchronously(r("1")))
  Assert.Equal("b", Async.RunSynchronously(r("2")))

let private batchQueryFn a = 
  match a with
  | "1" -> async { return Some("a") }
  | "2" -> async { return Some("b") }
  | _ -> async { return None }

let private batchQueryRepo = batchQueryRepository(batchQueryFn)

[<Fact>]
let ``batch query repository returns found keys in the found part of the result`` () = 
  let result = Async.RunSynchronously(batchQueryRepo(["1"; "2"]))
  Assert.Equal("a", result.found |> Seq.find(fun (k, _) -> k = "1") |> snd)

[<Fact>]
let ``batch query repository returns not found keys in the not found part of the result`` () = 
  let result = Async.RunSynchronously(batchQueryRepo(["foo"; "bar"]))
  Assert.Equal<string seq>(result.notFound, ["foo"; "bar"])

[<Fact>]
let ``batch query repository returns failures in the failed part of the result`` () = 
  let r = batchQueryRepository(failingRepository(exn "failure"))
  let result = Async.RunSynchronously(r(["foo"; "bar"]))
  Assert.True(result.failed |> Seq.exists(fun (k, _) -> k = "foo"))

[<Fact>]
let ``synchronous caching repository calls repository when cache is empty`` () = 
  let cache = ExtendEverything.Cache.Synchronous.EmptyCache<int, int>() :> ExtendEverything.Cache.Synchronous.ICache<int, int>
  let r = cachingRepository(cache, fun _ -> async { return 2 })
  Assert.Equal(2, Async.RunSynchronously(r(1)))

[<Fact>]
let ``synchronous caching repository doesn't call repository when cache is filled`` () = 
  let cache = new ExtendEverything.Cache.Synchronous.LRUCache<int, int>(1, [(1, 2)])
  let r = cachingRepository(cache, failingRepository(exn "should not be called"))
  Assert.Equal(2, Async.RunSynchronously(r(1)))

[<Fact>]
let ``synchronous caching repository fills the cache on a miss`` () = 
  let cache = new ExtendEverything.Cache.Synchronous.LRUCache<int, int>(1)
  let mutable called = false
  let repoFn _ = 
    if not called then 
      called <- true
      async { return 2 }
    else
      async { return(failwith "called twice") }

  let r = cachingRepository(cache, repoFn)
  Assert.Equal(2, Async.RunSynchronously(r(1)))

  // wait, to ensure that the cache has been filled asynchronously
  Thread.Sleep(TimeSpan(0, 0, 0, 0, 1))
  Assert.Equal(2, Async.RunSynchronously(r(1)))

[<Fact>]
let ``async caching repository calls repository when cache is empty`` () = 
  let cache = ExtendEverything.Cache.Asynchronous.EmptyCache<int, int>() :> ExtendEverything.Cache.Asynchronous.IAsyncCache<int, int>
  let r = asyncCachingRepository(cache, fun _ -> async { return 2 })
  Assert.Equal(2, Async.RunSynchronously(r(1)))

[<Fact>]
let ``async caching repository doesn't call repository when cache is filled`` () = 
  let cache = new ExtendEverything.Cache.Asynchronous.LRUCache<int, int>(1, [(1, 2)])
  let r = asyncCachingRepository(cache, failingRepository(exn "should not be called"))
  Assert.Equal(2, Async.RunSynchronously(r(1)))

[<Fact>]
let ``async caching repository fills the cache on a miss`` () = 
  let cache = new ExtendEverything.Cache.Asynchronous.LRUCache<int, int>(1)
  let mutable called = false
  let repoFn _ = 
    if not called then 
      called <- true
      async { return 2 }
    else
      async { return(failwith "called twice") }

  let r = asyncCachingRepository(cache, repoFn)
  Assert.Equal(2, Async.RunSynchronously(r(1)))

  // wait, to ensure that the cache has been filled asynchronously
  Thread.Sleep(TimeSpan(0, 0, 0, 0, 1))
  Assert.Equal(2, Async.RunSynchronously(r(1)))

[<Fact>]
let ``retrying repository retries failures`` () = 
  let mutable called = false
  let r = 
    fun _ -> 
      if not called then
        called <- true
        async { return(failwith "first failure") }
      else
        async { return 2 }

  let rr = retryingRepository(r, 1, (fun _ -> true), (fun _ -> 0))
  Assert.Equal(2, Async.RunSynchronously(rr(1)))

[<Fact>]
let ``retrying repository fails after retries are exhausted`` () = 
  let mutable called = false
  let ex = exn "second failure"
  let r = 
    fun _ -> 
      if not called then
        called <- true
        failwith "first failure"
      else
        raise ex

  let rr = retryingRepository(r, 1, (fun _ -> true), (fun _ -> 0))
  Assert.Throws<System.Exception>(new System.Action(Async.RunSynchronously(rr(1))))

[<Fact>]
let ``retrying repository fails when exceptions are not caught by the filter`` () = 
  let rr = retryingRepository(failingRepository(exn "always fails"), 1, (fun _ -> false), (fun _ -> 0))
  Assert.Throws<System.Exception>(new System.Action(Async.RunSynchronously(rr(1))))

[<Fact>]
let ``sideEffectOnSuccess runs the side effect on success`` () = 
  use ran = new ManualResetEvent(false)
  let se = fun _ -> async { ran.Set() |> ignore }
  let r = fun _ -> async { return 2 }
  let ser = sideEffectOnSuccess(r, se)

  Assert.Equal(2, Async.RunSynchronously(ser(1)))
  Assert.True(ran.WaitOne(500))

[<Fact>]
let ``sideEffectOnSuccess does not run the side effect on failure`` () = 
  use ran = new ManualResetEvent(false)
  let se = fun _ -> async { ran.Set() |> ignore }
  let ser = sideEffectOnSuccess(failingRepository(exn "always fails"), se)
  let mutable succeeded = false

  try
    Async.RunSynchronously(ser(1))
    succeeded <- true
  with
  | _ -> Assert.False(ran.WaitOne(1))

  Assert.False(succeeded)

[<Fact>]
let ``sideEffectOnFailure does not run the side effect on success`` () = 
  use ran = new ManualResetEvent(false)
  let se = fun _ -> async { ran.Set() |> ignore }
  let r = fun _ -> async { return 2 }
  let ser = sideEffectOnFailure(r, se)

  Async.RunSynchronously(ser(1)) |> ignore
  Assert.False(ran.WaitOne(1))

[<Fact>]
let ``sideEffectOnFailure runs the side effect on failure`` () = 
  use ran = new ManualResetEvent(false)
  let se = fun _ -> async { ran.Set() |> ignore }
  let ser = sideEffectOnFailure(failingRepository(exn "always fails"), se)
  let mutable succeeded = false

  try
    Async.RunSynchronously(ser(1))
    succeeded <- true
  with
  | _ -> Assert.True(ran.WaitOne(1))
  Assert.False(succeeded)

[<Fact>]
let ``timingRepository reports time after a query succeeds`` () = 
  let r = fun _ -> async { return 2 }
  let mutable succeeded = false
  let tr = 
    timingRepository(
      r,
      (fun _ -> DateTime.Now),
      (fun _ -> succeeded <- true)
    )

  Assert.Equal(2, Async.RunSynchronously(tr(1)))
  Assert.True(succeeded)

[<Fact>]
let ``timingRepository reports time even if the query fails`` () = 
  let r = fun _ -> async { failwith "always fails" }
  let mutable succeeded = false
  let tr = 
    timingRepository(
      r,
      (fun _ -> DateTime.Now),
      (fun _ -> succeeded <- true)
    )

  try
    Async.RunSynchronously(tr(2))
    succeeded <- false
  with
  | _ -> ()

  Assert.True(succeeded)

[<Fact>]
let ``comparingRepository runs the 'same' effect when the results are equal`` () = 
  let mutable ran1 = false
  let mutable ran2 = false
  let mutable ranSame = false
  let r1 = fun _ -> async { ran1 <- true; return 1 }
  let r2 = fun _ -> async { ran2 <- true; return 1 }

  let r = 
    comparingRepository(
      r1,
      r2,
      (fun _ -> ranSame <- true),
      (fun _ -> failwith "should not run")
    )

  Assert.Equal(1, Async.RunSynchronously(r(1)))
  Assert.True(ran1)
  Assert.True(ran2)
  Assert.True(ranSame)

[<Fact>]
let ``comparingRepository runs the 'different' effect when the results are equal`` () = 
  let mutable ran1 = false
  let mutable ran2 = false
  let mutable ranDifferent = false
  let r1 = fun _ -> async { ran1 <- true; return 1 }
  let r2 = fun _ -> async { ran2 <- true; return 2 }

  let r = 
    comparingRepository(
      r1,
      r2,
      (fun _ -> failwith "should not run"),
      (fun _ -> ranDifferent <- true)
    )

  Assert.Equal(1, Async.RunSynchronously(r(1)))
  Assert.True(ran1)
  Assert.True(ran2)
  Assert.True(ranDifferent)

[<Fact>]
let ``delayed waits the specified amount before running the query`` () = 
  let mutable ran = false
  let r = 
    fun _ -> 
      ran <- true
      async { return 2 }

  let d = delayed(r, fun _ -> TimeSpan(0, 0, 0, 0, 1))
  let test = async {
    let op = d(1)
    Assert.False(ran)
    let! _ = op
    Assert.True(ran)
    ()
  }

  Async.RunSynchronously(test)

[<Fact>]
let ``repositoryWithFallback doesn't use the fallback if there is no exception`` () = 
  let a = fun _ -> async { return 0 }
  let b = fun _ -> async { return 1 }
  let r = repositoryWithFallback(a, b)
  Assert.Equal(0, Async.RunSynchronously(r(0)))

[<Fact>]
let ``repositoryWithFallback uses the fallback if there is an exception`` () = 
  let a = fun _ -> async { return raise(exn "always fails") }
  let b = fun _ -> async { return 1 }
  let r = repositoryWithFallback(a, b)
  Assert.Equal(1, Async.RunSynchronously(r(0)))

[<Fact>]
let ``timeoutRepository does not cancel the query if it finishes before the timeout`` () = 
  let r(_: int, ct: CancellationToken) =
    async {
      let! wasCancelled = Async.AwaitWaitHandle(ct.WaitHandle, 1)
      Assert.False(wasCancelled)
      return 1
    }

  let tor = timeoutRepository(r, fun _ -> TimeSpan(0, 0, 0, 0, 50))
  Assert.Equal(1, Async.RunSynchronously(tor(1)))

[<Fact>]
let ``timeoutRepository cancels the query if it is not finished before the timeout`` () = 
  let r(_: int, ct: CancellationToken) = 
    async { 
      let! _ = Async.Sleep(2)
      return! Async.AwaitWaitHandle(ct.WaitHandle, 1)
    }

  let tor = timeoutRepository(r, fun _ -> TimeSpan(0, 0, 0, 0, 1))
  Assert.True(Async.RunSynchronously(tor(1)))

[<Fact>]
let ``timeWindowCircuitBreaking uses the main repository by default`` () = 
  let r _ = async { return 1 }
  let a _ = async { return raise(exn "should not be used") }
  let twcbr = 
    timeWindowCircuitBreaking(
      r, 
      a,
      1,
      new TimeSpan(0, 0, 0, 5, 0),
      0.95,
      fun _ -> DateTime.Now
    )

  Seq.init(10)(fun _ -> Assert.Equal(1, Async.RunSynchronously(twcbr(1))))

[<Fact>]
let ``timeWindowCircuitBreaking uses the alternate repository if the success rate is below the threshold`` () =
  let r _ = async { return raise(exn "should lower success rate below threshold") }
  let a _ = async { return 2 }
  let twcbr = 
    timeWindowCircuitBreaking(
      r,
      a,
      10,
      new TimeSpan(5000, 0, 0, 0, 0),
      0.99,
      fun _ -> DateTime.Now
    )

  match Async.RunSynchronously(Async.Catch(twcbr(1))) with
  | Choice2Of2 _ -> ()
  | _ -> failwith "first request should have thrown"
  
  Seq.init(10)(fun _ -> Assert.Equal(2, Async.RunSynchronously(twcbr(1))))

[<Fact>]
let ``timeWindowCircuitBreaking expires results outside of the time window`` () = 
  let r _ = async { return raise(exn "should lower success rate below threshold") }
  let a _ = async { return 2 }
  let mutable timeCount = 0
  let timeStart = DateTime.Now
  let twcbr = 
    timeWindowCircuitBreaking(
      r,
      a,
      10,
      new TimeSpan(0, 0, 0, 1, 0),
      0.99,
      fun _ -> 
        timeCount <- timeCount + 1
        timeStart + TimeSpan.FromSeconds(float timeCount)
    )

  match (Async.RunSynchronously(Async.Catch(twcbr(1))), Async.RunSynchronously(Async.Catch(twcbr(1)))) with
  | (Choice2Of2 _, Choice2Of2 _) -> ()
  | _ -> failwith "both requests should have thrown"
    

[<Fact>]
let ``timeWindowCircuitBreakingFailure fails with a known exception when the circuit is broken`` () = 
  let r _ = async { return raise(exn "should lower success rate below threshold") }
  let twcbr = 
    timeWindowCircuitBreakingFailure(
      r,
      10,
      new TimeSpan(5000, 0, 0, 0, 0),
      0.99,
      fun _ -> DateTime.Now
    )

  match Async.RunSynchronously(Async.Catch(twcbr(1))) with
  | Choice2Of2 _ -> ()
  | _ -> failwith "first request should have thrown"
  
  match Async.RunSynchronously(Async.Catch(twcbr(1))) with
  | Choice2Of2(CircuitBrokenException) -> ()
  | _ -> failwith "second request should have thrown CircuitBrokenException"
      

[<Fact>]
let ``windowed rate limited repository allows requests up to the supplied count`` () = 
  let r _ = async { return 1 }
  let testTime = DateTime.Now
  let wrlr = 
    windowedRateLimitedRepository(
      r,
      10,
      new TimeSpan(0, 0, 0, 1, 0),
      fun _ -> testTime
    )

  Seq.init(10)(fun _ -> Assert.Equal(1, Async.RunSynchronously(wrlr(1))))

[<Fact>]
let ``windowed rate limited repository raises an exception on requests above the supplied count`` () = 
  let r _ = async { return 1 }
  let testTime = DateTime.Now
  let wrlr = 
    windowedRateLimitedRepository(
      r,
      1,
      new TimeSpan(0, 0, 0, 1, 0),
      fun _ -> testTime
    )

  Assert.Equal(1, Async.RunSynchronously(wrlr(1)))
  let second = Async.RunSynchronously(Async.Catch(wrlr(1)))
  match second with
  | Choice2Of2(RateLimitException _) -> ()
  | _ -> failwith "second attempt should have failed"

[<Fact>]
let ``windowed rate limited repository expires results outside the time window`` () = 
  let r _ = async { return 1 }
  let testTime = DateTime.Now
  let mutable testCount = 0
  let wrlr = 
    windowedRateLimitedRepository(
      r,
      1,
      new TimeSpan(0, 0, 0, 1, 0),
      fun _ -> 
        testCount <- testCount + 1
        testTime + TimeSpan.FromSeconds(float testCount)
    )

  Assert.Equal(1, Async.RunSynchronously(wrlr(1)))
  Assert.Equal(1, Async.RunSynchronously(wrlr(1)))
