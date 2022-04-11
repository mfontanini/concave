# concave: (toy) OCC-based durable key/value store

## What

This is a toy key/value store that allows 2 operations:

* Read a key.
* Atomically create/update a set of key/value pairs.

Key/value pairs are versioned serially, and clients are required to send the latest version for each of the affected keys for a write to succeed. Therefore, the pattern for updating a key is:

```
let version = get(key).version;
write(key, value, version);
```

This means:
* Any writes that do not provide the latest/current version of the key/value pair will be rejected.
* Any attempts to modify a key that is concurrently being written to by a separate put request will be rejected.
* Any write operation only succeeds if none of the keys to be updated were found in the 2 states above. No partial
updates are made: writes either succeed and update all keys, or fail and their staged changes are rolled back.

## Durability

Write requests are asynchronously batched together and flushed to disk periodically using a write ahead log. Put requests wait for the associated write to be flushed to disk before returning, which means requests will not return until data is considered to be persisted successfully on disk.

All objects are serialized into protobuf models to reduce the size of the log.

### Compaction

Log files are periodically compacted together, persisting only the latest version for a key and dropping the stale ones.

## API

An HTTP API is exposed that allows the 2 operations;

### /v1/get/{key}

Returns the state of the key or replies with 404 if it's not found.

**Response (JSON)**
```
{
    "key": "<key>",
    "value": <value>,
    "version": <version>,
}
```

Note that values can be any of:

* String
* Integer
* Float
* Bool
* Array of numbers

### /v1/put

Attempts to modify a set of key/value pairs.

**Request body (JSON)**

```
[
    {
        "key": "<key>",
        "value": "<value>",
        "version": expected_version,
    },
    ...
]
```


**Response (JSON)**
```
{
    "result": "Success" | "Failure",
    "error": "<error message>", # only set on Failure
}
```
