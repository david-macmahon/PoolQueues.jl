using Documenter, PoolQueues

makedocs(
    sitename = "PoolQueues.jl",
    authors = "David MacMahon and contributors",
    modules = [PoolQueues],
    pages = [
        "Home" => "index.md",
        "Guide" => "guide.md",
        "API Reference" => "api.md",
    ],
    warnonly = false,
)

deploydocs(
    repo = "github.com/david-macmahon/PoolQueues.jl.git",
    devbranch = "main",
    push_preview = true,
)
