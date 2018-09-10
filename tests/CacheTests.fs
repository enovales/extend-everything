namespace composition

open System
open Xunit

module SynchronousCacheTests = 
  open ExtendEverything.Cache.Synchronous

  [<Fact>]
  let ``EmptyCache always returns None on get`` () = 
    let c = new EmptyCache<int, int>() :> ICache<int, int>
    Assert.Equal(None, c.Get(0))

  [<Fact>]
  let ``LRUCache returns a stored item`` () = 
    let cache = new LRUCache<int, int>(1) :> ICache<int, int>
    cache.Set(1)(2)
    Assert.Equal(Some(2), cache.Get(1))

  [<Fact>]
  let ``LRUCache forgets items beyond its capacity`` () = 
    let cache = new LRUCache<int, int>(1) :> ICache<int, int>
    cache.Set(1)(2)
    cache.Set(2)(3)
    Assert.Equal(None, cache.Get(1))

  [<Fact>]
  let ``LRUCache can clear a value`` () = 
    let cache = new LRUCache<int, int>(1) :> ICache<int, int>
    cache.Set(1)(2)
    cache.Clear(1)
    Assert.Equal(None, cache.Get(1))

  [<Fact>]
  let ``TTLCache returns a stored item`` () = 
    let mutable time = DateTime.Now
    let ttl = TimeSpan.FromSeconds(3.0)
    let cache = new TTLCache<int, int>(1, ttl, fun _ -> time) :> ICache<int, int>
    cache.Set(1)(2)
    time <- DateTime.MinValue
    Assert.Equal(Some(2), cache.Get(1))

  [<Fact>]
  let ``TTLCache items expire after their TTL`` () = 
    let mutable time = DateTime.Now
    let ttl = TimeSpan.FromSeconds(3.0)
    let cache = new TTLCache<int, int>(1, ttl, fun _ -> time) :> ICache<int, int>
    cache.Set(1)(2)
    time <- DateTime.MaxValue
    Assert.Equal(None, cache.Get(1))


module AsynchronousCacheTests = 
  open ExtendEverything.Cache.Asynchronous

  [<Fact>]
  let ``EmptyCache always returns None on get`` () = 
    let c = new EmptyCache<int, int>() :> IAsyncCache<int, int>
    Assert.Equal(None, Async.RunSynchronously(c.Get(0)))

  [<Fact>]
  let ``LRUCache returns a stored item`` () = 
    let cache = new LRUCache<int, int>(1) :> IAsyncCache<int, int>
    let op = async {
      let! _ = cache.Set(1)(2)
      return! cache.Get(1)
    }
    Assert.Equal(Some(2), Async.RunSynchronously(op))

  [<Fact>]
  let ``LRUCache forgets items beyond its capacity`` () = 
    let cache = new LRUCache<int, int>(1) :> IAsyncCache<int, int>
    let op = async {
      let! _ = cache.Set(1)(2)
      let! _ = cache.Set(2)(3)
      return! cache.Get(1)
    }
    Assert.Equal(None, Async.RunSynchronously(op))

  [<Fact>]
  let ``LRUCache can clear a value`` () = 
    let cache = new LRUCache<int, int>(1) :> IAsyncCache<int, int>
    let op = async {
      let! _ = cache.Set(1)(2)
      let! _ = cache.Clear(1)
      return! cache.Get(1)
    }
    Assert.Equal(None, Async.RunSynchronously(op))

  [<Fact>]
  let ``TTLCache returns a stored item`` () = 
    let mutable time = DateTime.Now
    let ttl = TimeSpan.FromSeconds(3.0)
    let cache = new TTLCache<int, int>(1, ttl, fun _ -> time) :> IAsyncCache<int, int>
    let op = async {
      let! _ = cache.Set(1)(2)
      time <- DateTime.MinValue
      return! cache.Get(1)
    }
    Assert.Equal(Some(2), Async.RunSynchronously(op))

  [<Fact>]
  let ``TTLCache items expire after their TTL`` () = 
    let mutable time = DateTime.Now
    let ttl = TimeSpan.FromSeconds(3.0)
    let cache = new TTLCache<int, int>(1, ttl, fun _ -> time) :> IAsyncCache<int, int>
    let op = async {
      let! _ = cache.Set(1)(2)
      time <- DateTime.MaxValue
      return! cache.Get(1)
    }
    Assert.Equal(None, Async.RunSynchronously(op))
