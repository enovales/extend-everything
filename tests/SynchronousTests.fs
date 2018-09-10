module Tests

open ExtendEverything.Synchronous
open ExtendEverything.Cache.Synchronous

open System
open System.Threading
open Xunit

[<Fact>]
let ``switching repository uses the A repository when the function returns true`` () =
  let f = fun _ -> true
  let a = fun _ -> "expected"
  let repo = switchingRepository(f, a, failingRepository(exn "wrong repo"))
  Assert.Equal("expected", repo("key"))    

[<Fact>]
let ``switching repository uses the B repository when the function returns false`` () = 
  let f = fun _ -> false
  let b = fun _ -> "expected"
  let repo = switchingRepository(f, failingRepository(exn "wrong repo"), b)
  Assert.Equal("expected", repo("key"))    

[<Fact>]
let ``mapping repository uses the correct repository for the input key`` () = 
  let f a = 
      match a with
      | "1" -> fun _ -> "a"
      | "2" -> fun _ -> "b"
      | _ -> failwith "unexpected"

  let r = mappingRepository(f)
  Assert.Equal("a", r("1"))
  Assert.Equal("b", r("2"))

let private batchQueryFn(a: string): string option = 
  match a with
  | "1" -> Some("a")
  | "2" -> Some("b")
  | _ -> None

let private batchQueryRepo = batchQueryRepository(batchQueryFn)

[<Fact>]
let ``batch query repository returns found keys in the found part of the result`` () = 
  let result = batchQueryRepo(["1"; "2"])
  Assert.Equal("a", result.found |> Seq.find(fun (k, _) -> k = "1") |> snd)

[<Fact>]
let ``batch query repository returns not found keys in the not found part of the result`` () = 
  let result = batchQueryRepo(["foo"; "bar"])
  Assert.Equal<string seq>(result.notFound, ["foo"; "bar"])

[<Fact>]
let ``batch query repository returns failures in the failed part of the result`` () = 
  let r = batchQueryRepository(failingRepository(exn "failure"))
  let result = r(["foo"; "bar"])
  Assert.True(result.failed |> Seq.exists(fun (k, _) -> k = "foo"))

[<Fact>]
let ``caching repository calls repository when cache is empty`` () = 
  let cache = new EmptyCache<int, int>() :> ICache<int, int>
  let r = cachingRepository(cache, fun _ -> 2)
  Assert.Equal(2, r(1))

[<Fact>]
let ``caching repository doesn't call repository when cache is filled`` () = 
  let cache = new LRUCache<int, int>(1, [(1, 2)])
  let r = cachingRepository(cache, failingRepository(exn "should not be called"))
  Assert.Equal(2, r(1))

[<Fact>]
let ``caching repository fills the cache on a miss`` () = 
  let cache = new LRUCache<int, int>(1)
  let mutable called = false
  let repoFn _ = 
    if not called then 
      called <- true
      2
    else
      failwith "called twice"

  let r = cachingRepository(cache, repoFn)
  Assert.Equal(2, r(1))
  Assert.Equal(2, r(1))

[<Fact>]
let ``retrying repository retries failures`` () = 
  let mutable called = false
  let r = 
    fun _ -> 
      if not called then
        called <- true
        failwith "first failure"
      else
        2

  let rr = retryingRepository(r, 1, (fun _ -> true))
  Assert.Equal(2, rr(1))

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

  let rr = retryingRepository(r, 1, (fun _ -> true))
  Assert.Throws<System.Exception>(new System.Action(rr(1)))

[<Fact>]
let ``retrying repository fails when exceptions are not caught by the filter`` () = 
  let rr = retryingRepository(failingRepository(exn "always fails"), 1, (fun _ -> false))
  Assert.Throws<System.Exception>(new System.Action(rr(1)))

[<Fact>]
let ``sideEffectOnSuccess runs the side effect on success`` () = 
  use ran = new ManualResetEvent(false)
  let se = fun _ -> ran.Set() |> ignore
  let r = fun _ -> 2
  let ser = sideEffectOnSuccess(r, se)

  Assert.Equal(2, ser(1))
  Assert.True(ran.WaitOne(500))

[<Fact>]
let ``sideEffectOnSuccess does not run the side effect on failure`` () = 
  use ran = new ManualResetEvent(false)
  let se = fun _ -> ran.Set() |> ignore
  let ser = sideEffectOnSuccess(failingRepository(exn "always fails"), se)
  let mutable succeeded = false

  try
    ser(1)
    succeeded <- true
  with
  | _ -> Assert.False(ran.WaitOne(1))

  Assert.False(succeeded)

[<Fact>]
let ``sideEffectOnFailure does not run the side effect on success`` () = 
  use ran = new ManualResetEvent(false)
  let se = fun _ -> ran.Set() |> ignore
  let r = fun _ -> 2
  let ser = sideEffectOnFailure(r, se)

  ser(1) |> ignore
  Assert.False(ran.WaitOne(1))

[<Fact>]
let ``sideEffectOnFailure runs the side effect on failure`` () = 
  use ran = new ManualResetEvent(false)
  let se = fun _ -> ran.Set() |> ignore
  let ser = sideEffectOnFailure(failingRepository(exn "always fails"), se)
  let mutable succeeded = false

  try
    ser(1)
    succeeded <- true
  with
  | _ -> Assert.True(ran.WaitOne(1))
  Assert.False(succeeded)

[<Fact>]
let ``timingRepository reports time after a query succeeds`` () = 
  let r = fun _ -> 2
  let mutable succeeded = false
  let tr = 
    timingRepository(
      r,
      (fun _ -> DateTime.Now),
      (fun _ -> succeeded <- true)
    )

  Assert.Equal(2, tr(1))
  Assert.True(succeeded)

[<Fact>]
let ``timingRepository reports time even if the query fails`` () = 
  let r = fun _ -> failwith "always fails"
  let mutable succeeded = false
  let tr = 
    timingRepository(
      r,
      (fun _ -> DateTime.Now),
      (fun _ -> succeeded <- true)
    )

  try
    tr(2)
    succeeded <- false
  with
  | _ -> ()

  Assert.True(succeeded)
