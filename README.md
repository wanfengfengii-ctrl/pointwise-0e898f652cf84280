# OSM PBF roads → importable polylines

`osm.roads(chunks, workdir)` streams an OSM PBF 0.6 byte sequence and returns
a closeable iterator of road polylines.  Pure Python standard library
(`sqlite3`, `zlib`, `struct`, `tempfile`); no third-party dependencies.

```python
import osm

with open("roads.osm.pbf", "rb") as stream:
    for road in osm.roads(stream, "scratch/"):
        print(road["id"], road["tags"], road["refs"], road["coords"])
```

## Contract

* `chunks` — a single-pass stream of `bytes`: an iterable of byte strings, a
  bytes-like object, or a binary file object.  Chunk boundaries are arbitrary
  and may split a protobuf message or a `Blob` at any byte.
* `workdir` — an existing directory used only for module-owned temporary files.
* Each yielded dict: `{"id": int, "tags": {str: str}, "refs": [int...],
  "coords": [[lon_nano, lat_nano], ...]}`.
* Ways are emitted in source order; only ways with a `highway` tag are
  emitted, but every `DenseNodes`/`Node` and every `Way` is decoded and
  validated.
* `refs` preserves the node sequence, including repeats.  `coords[i]` is the
  integer nanodegree `[longitude, latitude]` of `refs[i]`.
* Nothing is yielded until the whole stream is decoded **and** every
  reference of every retained way is confirmed present, so nodes may legitimately
  appear in Blobs after the ways that use them.
* Malformed input raises `ValueError` (truncation, bad framing, bad
  string-table indices, `DenseNodes` id/lat/lon count mismatches, broken
  zlib, missing nodes, oversized Blobs).  Exceptions raised by `chunks`
  propagate unchanged.
* Temporary files are removed on exhaustion, error, and `close()`.

## PBF support

* `OSMHeader` with required features limited to `OsmSchema-V0.6` and `DenseNodes`;
  unknown required features are rejected.
* `raw` and `zlib` Blob payloads; decoded payload limit is 32 MiB.
* `DenseNodes` (id/lat/lon deltas, `keys_vals` separators, `granularity`,
  `lat_offset`/`lon_offset`), plain `Node` messages, `Way` id/key/value/ref
  fields, packed or unpacked repeated fields, and per-block string tables.
* Relations are ignored; they are not needed for roads.

## Storage and memory

Nodes go to a `WITHOUT ROWID` SQLite table; retained ways go to an
append-only spill file on disk, with their ids indexed in the same database.
Parsing memory is bounded by one Blob plus one Way; emitted roads add
`O(one road)`.

## Running the checks

```sh
python3 verify.py                    # independent encoder + adversarial checks
python3 -m unittest discover -s tests  # unit tests
python3 demo/roads_demo.py           # late-node demo, cleanup demonstration
```
