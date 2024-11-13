
# Threading patterns

This repo is to remind me about some common threading patterns.
Mostly I forget that you can block on a queue.
I've got a perhaps overcomplicated example of how to set up workers
that consume from a queue.
The main thread starts the workers, seeds the queue, blocks on the queue
and then cleans up the workers when the queue is empty.
