"""
    nitems(c::Channel) -> number of items in `c`
    nitems(c::RemoteChannel) -> number of items in `c`
    nitems(pq::PoolQueue) -> (nitems(pq.pool), nitems(pq.queue))

Return the number of items available in `c`.
"""
nitems(c::Channel) = Base.n_avail(c)
nitems(c::RemoteChannel) = call_on_owner(channel_from_id, c) |> nitems
nitems(pq::PoolQueue) = (nitems(pq.pool), nitems(pq.queue))


"""
    maxsize(c::Channel) -> number of items that `c` can hold
    maxsize(c::RemoteChannel) -> number of items that `c` can hold
    maxsize(pq::PoolQueue) -> (maxsize(pq.pool), maxsize(pq.queue))

Return the maximum number of items that `c` can hold.
"""
maxsize(c::Channel) = c.sz_max
maxsize(c::RemoteChannel) = call_on_owner(channel_from_id, c) |> maxsize
maxsize(pq::PoolQueue) = (maxsize(pq.pool), maxsize(pq.queue))
