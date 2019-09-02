# kvs
In memory key value store.

## Disclaimer

This is my first stab at writing a Rust program, do not base yourself on this code. Or maybe do, worst case scenario you will learn what you shouldn't do. :P 

### Next steps

* When calling `::open()`
    * Create a HashMap with `key` and `CommandMetadata`, which may include:
        * stream.byte_offset()
        * command length

 * When calling `get` we need to:
    * Seek file to the byte position
    * Find the command length
    * Read bytes from `pos` to `pos + command_length`
    * Parse bytes into actual command
    * Return value or appropriate error
