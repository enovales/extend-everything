open System
open System.Net
open ExtendEverything.Asynchronous
open ExtendEverything.Cache.Asynchronous
open FSharp.Control

let private retrieveURI uri = 
    let uriObj = new Uri(uri)
    let webClient = new WebClient()
    Async.AwaitTask(webClient.DownloadStringTaskAsync(uriObj))

let mutable underlyingCounter = 0

let countingRepo = 
    fun u -> 
        underlyingCounter <- underlyingCounter + 1
        retrieveURI(u)

let retryingRetriever = retryingRepository(countingRepo, 1, (fun _ -> true), (fun _ -> 50))

let cache = new TTLCache<string, string>(10, TimeSpan(0, 0, 1, 0, 0), (fun _ -> DateTime.Now))
let cachingRetriever = asyncCachingRepository(cache, retryingRetriever)

[<EntryPoint>]
let main argv =
    let url = argv.[0]

    printfn "About to retrieve URL [%s]" url

    let result1 = Async.RunSynchronously(cachingRetriever(url))

    printfn "successfully retrieved URL [%s]" url

    let results = 
        Async.RunSynchronously(
            Async.Parallel<string>(
                Seq.init(10)(fun _ -> cachingRetriever(url))
            )
        )

    printfn "completed all subsequent calls."
    printfn "calls to underlying: %d" underlyingCounter

    let success =
        results
        |> Seq.forall(fun r -> r = result1)

    if success then
        printfn "All retrievals resulted in the same string:"
        printfn "------------------------------------------------------"
        printfn "%s" result1
        printfn "------------------------------------------------------"
    else
        printfn "Not all retrievals were the same"

    0
