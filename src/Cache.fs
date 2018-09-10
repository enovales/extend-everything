namespace ExtendEverything.Cache

module Synchronous = 
    (* cache for a repository *)
  type ICache<'a, 'b> = 
    abstract Get: 'a -> option<'b>
    abstract Set: 'a -> 'b -> unit
    abstract Clear: 'a -> unit

  (* A cache implementation that always returns empty *)
  type EmptyCache<'a, 'b>() = 
    interface ICache<'a, 'b> with
      member this.Get _ = None
      member this.Set _ _ = ()
      member this.Clear _ = ()

  (* A basic LRU cache implementation *)
  type LRUCache<'a, 'b when 'a : equality>(size: int, initialValues: list<'a * 'b>) =
    let mutable storage = initialValues
    new(size: int) = LRUCache(size, [])
    interface ICache<'a, 'b> with
      member this.Get(aKey: 'a) = storage |> Seq.tryFind (fst >> (=) aKey) |> Option.map snd
      member this.Set(aKey: 'a)(bVal: 'b) = 
        storage <- (aKey, bVal) :: (storage |> List.filter (fst >> (<>) aKey) |> List.take(size - 1))
        ()
      member this.Clear(aKey: 'a) = 
        storage <- (storage |> List.filter (fst >> (<>) aKey))
        ()

  type private TTLCacheRecord<'b> = 
    {
      value: 'b
      expiry: System.DateTime
    }

  (* A basic TTL cache implementation *)
  type TTLCache<'a, 'b when 'a : comparison>(size: int, ttl: System.TimeSpan, getTime: unit -> System.DateTime) = 
    let mutable storage = Map.empty<'a, TTLCacheRecord<'b>>
    interface ICache<'a, 'b> with
      member this.Get(aKey: 'a) = 
        storage 
        |> Map.tryFind(aKey) 
        |> Option.filter(fun cr -> cr.expiry > getTime()) 
        |> Option.map (fun cr -> cr.value)

      member this.Set(aKey: 'a)(bVal: 'b) = 
        let newExpiry = getTime() + ttl
        storage <- storage |> Map.add aKey { TTLCacheRecord.value = bVal; expiry = newExpiry}
        ()
      member this.Clear(aKey: 'a) = 
        storage <- storage |> Map.remove(aKey)
        ()

module Asynchronous = 
    (* cache for a repository *)
  type IAsyncCache<'a, 'b> = 
    abstract Get: 'a -> Async<option<'b>>
    abstract Set: 'a -> 'b -> Async<unit>
    abstract Clear: 'a -> Async<unit>

  (* A cache implementation that always returns empty *)
  type EmptyCache<'a, 'b>() = 
    interface IAsyncCache<'a, 'b> with
      member this.Get _ = async { return None }
      member this.Set _ _ = async { return () }
      member this.Clear _ = async { return () }

  (* A basic LRU cache implementation *)
  type LRUCache<'a, 'b when 'a : equality>(size: int, initialValues: list<'a * 'b>) =
    let mutable storage = initialValues
    new(size: int) = LRUCache(size, [])
    interface IAsyncCache<'a, 'b> with
      member this.Get(aKey: 'a) = async { return (storage |> Seq.tryFind (fst >> (=) aKey) |> Option.map snd) }
      member this.Set(aKey: 'a)(bVal: 'b) = 
        async {
          storage <- (aKey, bVal) :: (storage |> List.filter (fst >> (<>) aKey) |> List.take(size - 1))
          return ()
        }
      member this.Clear(aKey: 'a) = 
        async {
          storage <- (storage |> List.filter (fst >> (<>) aKey))
          return ()
        }

  type private TTLCacheRecord<'b> = 
    {
      value: 'b
      expiry: System.DateTime
    }

  (* A basic TTL cache implementation *)
  type TTLCache<'a, 'b when 'a : comparison>(size: int, ttl: System.TimeSpan, getTime: unit -> System.DateTime) = 
    let mutable storage = Map.empty<'a, TTLCacheRecord<'b>>
    interface IAsyncCache<'a, 'b> with
      member this.Get(aKey: 'a) = 
        async {
          return storage 
            |> Map.tryFind(aKey) 
            |> Option.filter(fun cr -> cr.expiry > getTime()) 
            |> Option.map (fun cr -> cr.value)
        }

      member this.Set(aKey: 'a)(bVal: 'b) = 
        let newExpiry = getTime() + ttl
        async {
          storage <- storage |> Map.add aKey { TTLCacheRecord.value = bVal; expiry = newExpiry}
          return ()
        }
      member this.Clear(aKey: 'a) = 
        async {
          storage <- storage |> Map.remove(aKey)
          return ()
        }