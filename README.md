# kvs

Log structured key-value store.

_Reading_

During startup the data is read from every log file and stored in a BTreeMap in which keys points directly to locations on disk where the data lives.
Read operations uses at most one disk seek, sometimes none due to file system caching.

_Writing_

Data is written to append only files and requires two seek operations to the end of the file.
This will be refactored in order to require a single seek operation.

## Disclaimer

This is my first stab at writing a Rust program, do not base yourself on this code. Or maybe do, worst case scenario you will learn what you shouldn't do. :P

### Next steps

#### Engine

- ~~Consider using BTreeMap instead of HashMap~~
- ~~Use BufWriter instead of a file directly~~
- ~~Extract error related logic to a proper file~~
- ~~Have proper reader and writer files instead of a single file~~
- ~~Refactor metadata logic, maybe extending BufReader itself~~
- ~~Implement compaction algorithm~~
- ~~Trigger compaction during set operation~~

#### Server

- ~~Bind to the specified address~~
- ~~Start listening to requests~~
- ~~Parse and consume requests~~
- ~~Serve response~~

#### Client

- ~~Connect to server address~~
- ~~Send requests~~
- ~~Parse response~~
- ~~Return response~~

#### Protocol

- ~~Flesh out structs for the protocol~~
- ~~Check if we can use Serde to serialize/deserialize structs and write/consume from stream~~

#### Error Handling

- ~~Client~~
- ~~Server~~
- ~~Binaries~~

#### Nice to have

- Should we use multiple log files with a bounded size?
- Refactor set operation to require a single seek
