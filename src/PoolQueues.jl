"""
PoolQueues facilitate sharing pools of items between producer Tasks and consumer
Tasks.  See `PoolQueue` for more information.
"""
module PoolQueues

using Distributed: RemoteChannel, call_on_owner, channel_from_id

export PoolQueue
export acquire!
export produce!
export consume!
export recycle!
export nitems
export maxsize

const PQChannel{T} = Union{AbstractChannel{T},
                           RemoteChannel{<:AbstractChannel{T}}}

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

    # Prepare item for consumer task (application specific)

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
    recycle!(poolqueue, item)
end
```

# Example use case

One usage scenario is for the producer task to read a portion of a data file,
send that to the consumer task for processing, and then read the next portion of
the data file.  The producer Task's reading of the next portion of data happens
in parallel with the consumer Task's processing the previous data.  Instead of:

```text
main task: read1 process1 read2 process2 read3 process3 ... [time -->]
```

using a PoolQueue with two (or more) items allows:

```text
producer task: read1 read2    read3    ... [time -->]
consumer task:       process1 process2 ... [time -->]
```
"""
struct PoolQueue{Tp,Tq,Cp<:PQChannel{Tp},Cq<:PQChannel{Tq}}
    pool::Cp
    queue::Cq

    PoolQueue{Tp,Tq}(p::Cp, q::Cq) where {Tp, Tq, Cp<:PQChannel{Tp},
                                          Cq<:PQChannel{Tq}} = new{Tp,Tq,Cp,Cq}(p, q)
end

"""
    PoolQueue(p::Cp, q::Cq) where {Tp, Cp<:PQChannel{Tp},
                                   Tq, Cq<:PQChannel{Tq}}

Construct a PoolQueue from two `PQChannel` instances.  The element type of `p`
must be `Tp` and the element type of `q` must be `Tq`.
"""
function PoolQueue(p::Cp, q::Cq) where {Tp, Cp<:PQChannel{Tp},
                                        Tq, Cq<:PQChannel{Tq}}
    PoolQueue{Tp,Tq}(p, q)
end

"""
    PoolQueue{Tp, Tq}(np::Integer, nq::Integer=np)

Construct a PoolQueue using a `Channel{Tp}` channel for the pool and a
`Channel{Tq}` channel for the queue.  The pool channel will hold up to `np`
items of type `Tp` and the queue channel will hold up to `nq` items of
type `Tq`.
"""
function PoolQueue{Tp,Tq}(np::Integer, nq::Integer=np) where {Tp,Tq}
    np > 0 || throw(ArgumentError("pool size must be positive"))
    nq > 0 || throw(ArgumentError("queue size must be positive"))
    PoolQueue(Channel{Tp}(np), Channel{Tq}(nq))
end

"""
    PoolQueue{Tp,Tq}(f::Function, np::Integer, nq::Integer=np, fargs...; fkwargs...)
    PoolQueue(f::Function, np::Integer, nq::Integer=np, fargs...; fkwargs...)

Construct a PoolQueue using a `Channel{Tp}` channel for the pool and a
`Channel{Tq}` channel for the queue.  The pool channel will hold up to `np`
items of type `Tp` and the queue channel will hold up to `nq` items of type
`Tq`.  The function `f`, which should return a single item of type `Tp`, will be
called `np` times as `f(fargs...; fkwargs...)` to pre-populate the PoolQueue's
pool.  If `fargs` is used, `nq` must be passed explicitly.  The
non-parameterized constructor uses the return type of `f` as `Tp` and `Tq`.

For the non-parameterized constructor, `f` must return the same concrete type
on every call.  If `f` may return values of varying types, use the explicit
`PoolQueue{Tp,Tq}(f, ...)` form instead.
"""
function PoolQueue{Tp,Tq}(f::Function, np::Integer, nq::Integer=np, fargs...; fkwargs...) where {Tp,Tq}
    pq = PoolQueue{Tp,Tq}(np, nq)
    for _ in 1:np
        recycle!(pq, f(fargs...; fkwargs...))
    end
    pq
end

function PoolQueue(f::Function, np::Integer, nq::Integer=np, fargs...; fkwargs...)
    item = f(fargs...; fkwargs...)
    T = typeof(item)
    pq = PoolQueue{T,T}(np, nq)
    recycle!(pq, item)
    for _ in 2:np
        recycle!(pq, f(fargs...; fkwargs...))
    end
    pq
end

"""
    PoolQueue(::Type{Tp}, [::Type{Tq},] np::Integer, nq::Integer=np, Tpargs...; Tpkwargs...)

Construct a PoolQueue using a `Channel{Tp}` channel for the pool and a
`Channel{Tq}` channel for the queue.  The pool channel will hold up to `np`
items of type `Tp` and the queue channel will hold up to `nq` items of type
`Tq`.  The constructor of `Tp` will be called `np` times as `Tp(Tpargs...;
Tpkwargs...)` to pre-populate the PoolQueue's pool.  If `Tpargs` is used, `nq`
must be passed explicitly.  If `Tq` is omitted, it will be taken to be the same
as `Tp`.
"""
function PoolQueue(::Type{Tp}, ::Type{Tq}, np::Integer, nq::Integer=np, Tpargs...; Tpkwargs...) where {Tp,Tq}
    PoolQueue{Tp,Tq}((a...; k...)->Tp(a...; k...), np, nq, Tpargs...; Tpkwargs...)
end

function PoolQueue(::Type{Tp}, np::Integer, nq::Integer=np, Tpargs...; Tpkwargs...) where {Tp}
    PoolQueue{Tp,Tp}((a...; k...)->Tp(a...; k...), np, nq, Tpargs...; Tpkwargs...)
end

"""
    Base.close(pq::PoolQueue)

Close the `queue` and `pool` channels associated with `pq`, in that order.
The PoolQueue is unusable after `close`; further `acquire!`/`produce!`/
`consume!`/`recycle!` operations will throw.
"""
function Base.close(pq::PoolQueue)
    close(pq.queue)
    close(pq.pool)
end

"""
    acquire!(pq::PoolQueue{Tp})::Tp where {Tp}

Acquire an available item from `pq.pool`.
"""
function acquire!(pq::PoolQueue{Tp})::Tp where {Tp}
    take!(pq.pool)
end

"""
    produce!(pq::PoolQueue{Tp,Tq}, item::Tq)::Tq where {Tp,Tq}

Produce `item` to `pq.queue`.
"""
function produce!(pq::PoolQueue{Tp,Tq}, item::Tq)::Tq where {Tp,Tq}
    put!(pq.queue, item)
    return item
end

"""
    produce!(f::Function, pq::PoolQueue{Tp,Tq}, fargs...)::Union{Nothing,Tq} where {Tp,Tq}

Produce an item by acquiring an available item from `pq.pool`, call `f(item,
fargs...)`, and `produce!` the value returned by `f` unless it is `nothing`.  If
`f` returns `nothing`, the item is recycled without being produced.  The value
returned by `f`, which is of type `Tq` or `nothing`, is returned from this
function.  If `f` throws an exception, the acquired item is recycled back to
`pq.pool` and the exception is rethrown.
"""
function produce!(f::Function, pq::PoolQueue{Tp,Tq}, fargs...)::Union{Nothing,Tq} where {Tp,Tq}
    poolitem = acquire!(pq)
    queueitem = try
        f(poolitem, fargs...)
    catch e
        recycle!(pq, poolitem)
        rethrow(e)
    end
    queueitem === nothing ? recycle!(pq, poolitem) : produce!(pq, queueitem)
    queueitem
end

"""
    consume!(pq::PoolQueue{Tp,Tq})::Tq where {Tp,Tq}

Consume an item from `pq.queue`.
"""
function consume!(pq::PoolQueue{Tp,Tq})::Tq where {Tp,Tq}
    take!(pq.queue)
end

"""
    consume!(f::Function, pq::PoolQueue{Tp,Tq}, fargs...)::Union{Nothing,Tp} where {Tp,Tq}

Consume an item from `pq.queue` and call `f(item, fargs...)`, which must be of
type `Tp` or `nothing`.  If the returned value is not `nothing` it will be
passed to `recycle!` to put it back in the pool.  If `f` throws an exception,
the consumed item is recycled back to `pq.pool` when `queueitem isa Tp` and the
exception is rethrown; otherwise the item cannot be recycled and is dropped
(with a warning) before rethrowing.
"""
function consume!(f::Function, pq::PoolQueue{Tp,Tq}, fargs...)::Union{Nothing,Tp} where {Tp,Tq}
    queueitem = consume!(pq)
    poolitem = try
        f(queueitem, fargs...)
    catch e
        if queueitem isa Tp
            recycle!(pq, queueitem)
        else
            @warn "consume!: f threw an exception and the consumed item cannot be recycled (Tq is not a subtype of Tp); dropping item" Tp=Tp Tq=Tq
        end
        rethrow(e)
    end
    # If poolitem is not `nothing`, recycle! it
    poolitem !== nothing && recycle!(pq, poolitem)
    poolitem
end

"""
    recycle!(pq::PoolQueue{Tp}, item::Tp)::Tp where {Tp}

Recycle `item` back to `pq.pool`.
"""
function recycle!(pq::PoolQueue{Tp}, item::Tp)::Tp where {Tp}
    put!(pq.pool, item)
    return item
end

include("utils.jl")

end # module PoolQueues
