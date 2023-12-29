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
struct PoolQueue{Cp,Cq}
    pool::Cp
    queue::Cq
    PoolQueue{Cp,Cq}(p::Cp, q::Cq) where {Tp, Cp<:AbstractChannel{Tp},
                                          Tq, Cq<:AbstractChannel{Tq}} = new(p, q)
end

"""
    PoolQueue(p::Cp, q::Cq) where {Cp<:AbstractChannel,
                                   Cq<:AbstractChannel}

Construct a PoolQueue from two `AbstractChannel{T}` instances.
"""
function PoolQueue(p::Cp, q::Cq) where {Cp<:AbstractChannel,
                                        Cq<:AbstractChannel}
    PoolQueue{Cp,Cq}(p, q)
end

"""
    PoolQueue{Tp, Tq}(np::Integer, nq::Integer=np)

Construct a PoolQueue using a `Channel{Tp}` channel for the pool and a
`Channel{Tq}` channel for the queue.  The pool channel will hold up to `np`
items of type `Tp` and the queue channel will hold up to `np` items of
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
Close the `pool` and `queue` channels associated with `pq`.
"""
function Base.close(pq::PoolQueue)
    close(pq.queue)
    close(pq.pool)
end

"""
    acquire!(pq::PoolQueue{Cp,Cq})::Tp where {Tp, Cp<:AbstractChannel{Tp},
                                                  Cq<:AbstractChannel}

Acquire an available item from `pq.pool`.
"""
function acquire!(pq::PoolQueue{Cp,Cq})::Tp where {Tp, Cp<:AbstractChannel{Tp},
                                                       Cq<:AbstractChannel}
    take!(pq.pool)
end

"""
    produce!(pq::PoolQueue{Cp,Cq}, item::Tq)::Tq where {Tq, Cp<:AbstractChannel,
                                                            Cq<:AbstractChannel{Tq}}

Produce `item` to `pq.queue`.
"""
function produce!(pq::PoolQueue{Cp,Cq}, item::Tq)::Tq where {Tq, Cp<:AbstractChannel,
                                                                 Cq<:AbstractChannel{Tq}}
    put!(pq.queue, item)
end

"""
    produce!(f::Function, pq::PoolQueue{Cp,Cp}, fargs...)::Union{Tq,Nothing} where {Tq, Cp<:AbstractChannel,
                                                                                        Cq<:AbstractChannel{Tq}}

Produce an item by acquiring an available item from `pq.pool`, call `f(item,
fargs...)`, and `produce!` the value returned by `f` unless it is `nothing`.  If
`f` returns `nothing`, the item is recycled without being produced.  The value
returned by `f`, which is of type `Tq` or `nothing`, is returned from this
function.
"""
function produce!(f::Function, pq::PoolQueue{Cp,Cq}, fargs...)::Union{Tq, Nothing} where {Tq, Cp<:AbstractChannel,
                                                                                              Cq<:AbstractChannel{Tq}}
    poolitem = acquire!(pq)
    produceitem = f(poolitem, fargs...)
    produceitem === nothing ? recycle!(pq, poolitem) : produce!(pq, produceitem)
    produceitem
end

"""
    consume!(pq::PoolQueue{Cp,Cq})::Tq where {Tq, Cp<:AbstractChannel,
                                                  Cq<:AbstractChannel{Tq}}

Consume an item from `pq.queue`.
"""
function consume!(pq::PoolQueue{Cp,Cq})::Tq where {Tq, Cp<:AbstractChannel,
                                                       Cq<:AbstractChannel{Tq}}
    take!(pq.queue)
end

"""
    consume!(f::Function, pq::PoolQueue{Cp,Cq}, fargs...)::Tp where {Tp, Cp<:AbstractChannel{Tq},
                                                                         Cq<:AbstractChannel}

Consume an item from `pq.queue`, call `f(item, fargs...)`, and `recycle!` the
value returned by `f`, which must be of type `Tp`, back into the pool.
"""
function consume!(f::Function, pq::PoolQueue{Cp,Cq}, fargs...)::Tp where {Tp, Cp<:AbstractChannel{Tp},
                                                                              Cq<:AbstractChannel}
    consume!(pq) |> item->f(item, fargs...) |> item->recycle!(pq, item)
end

"""
    recycle!(pq::PoolQueue{Cp,Cq}, item::Tp)::Tp where {Tp, Cp<:AbstractChannel{Tp},
                                                            Cq<:AbstractChannel}

Recycle `item` back to `pq.pool`.
"""
function recycle!(pq::PoolQueue{Cp,Cq}, item::Tp)::Tp where {Tp, Cp<:AbstractChannel{Tp},
                                                                 Cq<:AbstractChannel}
    put!(pq.pool, item)
end

end # module PoolQueues
