"""
PoolQueues facilitate sharing pools of items between producer Tasks and consumer
Tasks.  See `PoolQueue` for more information.
"""
module PoolQueues

export PoolQueue
export acquire!
export produce!
export consume!
export recycle!
export produce_on_command

"""
A PoolQueue facilitates sharing a pool of items between a producer Task and a
consumer Task.  The four main operations on a PoolQueue are: `acquire!`,
`produce!`, `consume!`, and `recycle!`.  The first two are called by producer
Tasks; the latter two by consumer Tasks.  Typically the PoolQueue's pool is
pre-populated with preallocated items (such as Arrays or user defined structs).
PoolQueue constructor methods exist to facilitate this.  By recycling these
items in the PoolQueue, memory allocations (and garbage collection) can be
minimized.

The general flow for the producer Task is:

```julia
while true
    # Acquire an available item from the PoolQueue's pool
    item = acquire!(poolqueue)

    # Preprare item for consumer task (application specific)

    # Produce the item to the PoolQueue's queue
    produce!(poolqueue, item)
end
```

The general flow for the consumer Task is:

```julia
while true
    # Consume an item from the PoolQueue's queue
    item = consume!(poolqueue)

    # Process the item (application specific)

    # Recycle the item back to the PoolQueue's pool
    produce!(poolqueue, item)
end
```

# Example use case

One usage scenario is for the producer task to read a portion of a data file,
send that to the consumer task for processing, and then read the next portion of
the data file.  The producer Task's reading of the next portion of data happens
in parallel with the consumer Task's processing the previous data.  Instead of:

```text
main task: read0 process0 read1 process1 read2 process2 ... [time -->]
```

using a PoolQueue with two (or more) items allows:

```text
producer task: read1 read2    read3    ... [time -->]
consumer task:       process1 process2 ... [time -->]
```
"""
struct PoolQueue{C}
    pool::C
    queue::C
    PoolQueue{C}(p::C, q::C) where {T, C<:AbstractChannel{T}} = new(p, q)
end

"""
    PoolQueue(p::C, q::C) where {T, C<:AbstractChannel{T}}

Construct a PoolQueue from two `AbstractChannel{T}` instances.
"""
function PoolQueue(p::C, q::C) where {T, C<:AbstractChannel{T}}
    PoolQueue{C}(p, q)
end

"""
    PoolQueue{T}(np::Integer, nq::Integer=np) where {T}

Construct a PoolQueue using `Channel{T}` channels.  The pool channel will hold
up to `np` items of type `T` and the queue channel will hold up to `np` items of
type `T`.
"""
function PoolQueue{T}(np::Integer, nq::Integer=np) where {T}
    np > 0 || throw(ArgumentError("pool size must be positive"))
    nq > 0 || throw(ArgumentError("queue size must be positive"))
    PoolQueue{Channel{T}}(Channel{T}(np), Channel{T}(nq))
end

"""
    PoolQueue{T}(f::Function, np::Integer, nq::Integer=np, fargs...; fkwargs...) where {T}

Construct a PoolQueue using `Channel{T}` channels.  The pool channel will hold
up to `np` items of type `T` and the queue channel will hold up to `np` items of
type `T`.  The function `f`, which should return a single item of type `T`, will
be called `np` times as `f(fargs...; fkwargs...)` to pre-populate the
PoolQueue's pool.  If `fargs` is used, `nq` must be passed explicitly.
"""
function PoolQueue{T}(f::Function, np::Integer, nq::Integer=np, fargs...; fkwargs...) where {T}
    pq = PoolQueue{T}(np, nq)
    for _ in 1:np
        recycle!(pq, f(fargs...; fkwargs...))
    end
    pq
end

"""
    PoolQueue(::Type{T}, np::Integer, nq::Integer=np, fargs...; fkwargs...) where {T}

Construct a PoolQueue using `Channel{T}` channels.  The pool channel will hold
up to `np` items of type `T` and the queue channel will hold up to `np` items of
type `T`.  The constructor of `T` will be called `np` times as `T(fargs...;
fkwargs...)` to pre-populate the PoolQueue's pool.  If `fargs` is used, `nq` must
be passed explicitly.
"""
function PoolQueue(::Type{T}, np::Integer, nq::Integer=np, fargs...; fkwargs...) where {T}
    PoolQueue{T}((a...; k...)->T(a...; k...), np, nq, fargs...; fkwargs...)
end

"""
Close the `pool` and `queue` channels associated with `pq`.
"""
function Base.close(pq::PoolQueue)
    close(pq.queue)
    close(pq.pool)
end

"""
    acquire!(pq::PoolQueue{C})::T where {T, C<:AbstractChannel{T}}

Acquire an available item from `pq.pool`.
"""
function acquire!(pq::PoolQueue{C})::T where {T, C<:AbstractChannel{T}}
    take!(pq.pool)
end

"""
    produce!(pq::PoolQueue{C}, item::T)::T where {T, C<:AbstractChannel{T}}

Produce `item` to `pq.queue`.
"""
function produce!(pq::PoolQueue{C}, item::T)::T where {T, C<:AbstractChannel{T}}
    put!(pq.queue, item)
end

"""
    produce!(f::Function, pq::PoolQueue{C}, fargs...)::T where {T, C<:AbstractChannel{T}}

Produce an item by acquiring an available item from `pq.pool`, call `f(item,
fargs...)`, and `produce!` the value returned by `f` unless it is `nothing`.  If
`f` returns `nothing`, the item is recycled without being produced.  The value
returned by `f`, which is of type `T` or `nothing`, is returned from this
function.  returned by this call.
"""
function produce!(f::Function, pq::PoolQueue{C}, fargs...)::Union{T, Nothing} where {T, C<:AbstractChannel{T}}
    poolitem = take!(pq.pool)
    produceitem = f(poolitem, fargs...)
    produceitem === nothing ? put!(pq.pool, poolitem) : put!(pq.queue, produceitem)
    produceitem
end

"""
    consume!(pq::PoolQueue{C})::T where {T, C<:AbstractChannel{T}}

Consume an item from `pq.queue`.
"""
function consume!(pq::PoolQueue{C})::T where {T, C<:AbstractChannel{T}}
    take!(pq.queue)
end

"""
    consume!(f::Function, pq::PoolQueue{C}, fargs....)::T where {T, C<:AbstractChannel{T}}

Consume an item from `pq.queue`, call `f(item, fargs...)`, and `recycle!` the
value returned by `f`.
"""
function consume!(f::Function, pq::PoolQueue{C}, fargs...)::T where {T, C<:AbstractChannel{T}}
    take!(pq.queue) |> item->f(item, fargs...) |> item->put!(pq.pool, item)
end

"""
    recycle!(pq::PoolQueue{C}, item::T)::T where {T, C<:AbstractChannel{T}}

Recycle `item` back to `pq.pool`.
"""
function recycle!(pq::PoolQueue{C}, item::T)::T where {T, C<:AbstractChannel{T}}
    put!(pq.pool, item)
end

"""
    produce_on_command(produce, cmd::AbstractChannel{T}, pq::PoolQueue{C};
                       autoclose=true)

For each `command` taken from `cmd`, call `produce(command, pq)`.  This function
will typically be called within a task:

    producer_task = @task produce_on_command(myproducerfunc, cmd, pq)

If an exception is thrown while taking from `cmd`, an `@info` message is logged
and the funtion will return.  If an exception is thrown within `produce`, a
`@warn` message is logged at the function will return.  If `autoclose` is
`true` (the default), then `cmd` and `pq` will be closed before returning.
"""
function produce_on_command(produce, cmd::AbstractChannel{T}, pq::PoolQueue{C};
                            autoclose=true
                           ) where {T<:AbstractString, C<:AbstractChannel}
    # Command loop
    while true
        command = try
            # Take rawstem from cmd channel
            take!(cmd)
        catch
            @info "got exception from command channel [done]"
            break
        end

        @debug "calling $produce with command $command"
        try
            produce(command, pq)
        catch
            @warn "got exception from produce function [done]"
            break
        end
    end

    if autoclose
        close(pq.queue)
        close(cmd)
    end
    nothing
end

end # module PoolQueues
