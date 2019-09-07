# kvs
In memory key value store.

## Disclaimer

This is my first stab at writing a Rust program, do not base yourself on this code. Or maybe do, worst case scenario you will learn what you shouldn't do. :P

### Next steps

* Consider using BTreeMap instead of HashMap
* Should we use multiple log files with a bounded size?
* Use BufWriter instead of a file directly
* ~~Extract error related logic to a proper file~~
* Have proper reader and writer files instead of a single file
* Refactor metadata logic, maybe extending BufReader itself
* Implement compaction algorithm
