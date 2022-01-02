# PoolQueues

PoolQueues facilitate sharing pools of items between producer Tasks and consumer
Tasks.

The two main operations on a PoolQueue are production and consumption.
Production involves the `acquire!` and `produce!` functions.  Consumption
involves the `consume!` and `recycle!` functions.  The `produce!` and
`consume!` functions have methods accepting a `Function`.  These methods will
automatically ensure that `acquire!` and `recycle!` are called appropriately.
By using these methods, the user will not need to call `acquire!` or `recycle!`
directly.

Typically the PoolQueue's pool is pre-populated with preallocated items (such
as Arrays or user defined structs).  PoolQueue constructor methods exist to
facilitate this.  By recycling these items in the PoolQueue, memory allocations
(and garbage collection) can be minimized.

## Producer tasks

The conceptual flow for the producer Task is:

```julia
while true
    # Acquire an available item from the PoolQueue's pool
    item = acquire!(poolqueue)

    # Preprare item for production
    # (application specific code goes here)

    # Produce the item to the PoolQueue's queue
    produce!(poolqueue, item)
end
```

But this can be more conveniently expressed as:

```julia
while true
    produce!(poolqueue) do item
        # Preprare item for production
        # (application specific code goes here)

        # Return item for production
        return item
    end
end
```

## Consumer tasks

The conceptual flow for the consumer Task is:

```julia
while true
    # Consume an item from the PoolQueue's queue
    item = consume!(poolqueue)

    # Process the item
    # (application specific code goes here)

    # Recycle the item back to the PoolQueue's pool
    produce!(poolqueue, item)
end
```

But this can be more conveniently expressed as:

```julia
while true
    consume!(poolqueue) do item
        # Process the item
        # (application specific code goes here)

        # Return item for automatic recycling
        return item
    end
end
```

# Example use case

One usage scenario is for the producer task to read a portion of a data file,
send that to the consumer task for processing, and then read the next portion of
the data file.  The producer Task's reading of the next portion of data happens
in parallel with the consumer Task's processing the previous data.  Instead of:

    main task: read0 process0 read1 process1 read2 process2 ... [time -->]

using a PoolQueue with two (or more) items allows:

    producer task: read0 read1    read2    ... [time -->]
    consumer task:       process0 process1 ... [time -->]
