# Lab 1: Implement MapReduce

Based on [original Google paper](https://static.googleusercontent.com/media/research.google.com/lt//archive/mapreduce-osdi04.pdf)

Worker calls coordinator, signaling that it is available for a new task.

Coordinator responds with a task type, task related args.

Possible task types:
- Mapper: worker accepts a filename and reducer count `nReduce`. Worker inputs filename as key and file contents as value to the map function and receives a key-value list as output. It partitions the key-value list to `nReduce` intermediate files
- Reducer: worker receives its reducer index. Worker merges and sorts intermediate files belonging to this reducer by the index.
While iterating through key-value pairs, groups by keys and reduces.
- Idle: worker sleeps for 1 second, waiting for other mappers or reducers to finish. Worker should stay alert in case other worker becomes unresponsive

Worker count can be independent of desired mapper or reducer count

To keep track of state of the tasks, coordinator uses a data structure similar to LRU cache. It provides operations to add, remove and get earliest issued task context in constant time.