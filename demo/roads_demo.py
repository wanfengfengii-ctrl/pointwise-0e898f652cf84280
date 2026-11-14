"""Demonstration: OSM PBF ways -> importable polylines, nodes arriving late.

The scenario is written with ``tests/pbf_encode.py``, an independent PBF 0.6
encoder that shares no code with ``osm.py``.  Ways are placed in the first
OSMData Blob and reference nodes that only appear in later Blobs.  The stream
is then fed to ``osm.roads`` in irregular byte slices to show that Blob and
protobuf boundaries need not line up with chunk boundaries.

Run from the repository root:

    python3 demo/roads_demo.py
"""

import os
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tests"))

import osm  # noqa: E402
import pbf_encode as E  # noqa: E402

# (way id, tags, refs) in source order; nodes appear later in the stream.
WAYS = [
    (900, [("highway", "residential"), ("name", "County Road")],
     [1001, 1002, 1003, 1002, 1004]),
    (901, [("railway", "abandoned")], [1001, 1002]),  # not retained
    (902, [("highway", "service"), ("name", "Depot Yard")],
     [1003, 1003, 1005]),
]

# (node id, lon_nano, lat_nano)
NODE_GROUPS = [
    [(1001, 500_000_000, 250_000_000, []),
     (1002, 500_000_100, 250_000_100, [])],
    [(1003, 500_000_300, 250_000_200, []),
     (1004, 500_000_400, 250_000_100, []),
     (1005, 500_000_200, 250_000_300, [])],
]

EXPECTED = [
    {
        "id": 900,
        "tags": {"highway": "residential", "name": "County Road"},
        "refs": [1001, 1002, 1003, 1002, 1004],
        "coords": [[500_000_000, 250_000_000],
                   [500_000_100, 250_000_100],
                   [500_000_300, 250_000_200],
                   [500_000_100, 250_000_100],
                   [500_000_400, 250_000_100]],
    },
    {
        "id": 902,
        "tags": {"highway": "service", "name": "Depot Yard"},
        "refs": [1003, 1003, 1005],
        "coords": [[500_000_300, 250_000_200],
                   [500_000_300, 250_000_200],
                   [500_000_200, 250_000_300]],
    },
]


def build_stream(late_nodes=True):
    encoder = E.Encoder()
    blocks = [("OSMHeader", E.header_block())]

    ways_group = b"".join(
        E.f_bytes(3, encoder.way(way_id, tags, refs))
        for way_id, tags, refs in WAYS
    )
    blocks.append(("OSMData", E.primitive_block([ways_group], encoder.strings)))

    for index, nodes in enumerate(NODE_GROUPS):
        if not late_nodes and index == len(NODE_GROUPS) - 1:
            break
        group = E.f_bytes(2, encoder.dense(nodes))
        blocks.append(("OSMData", E.primitive_block([group], encoder.strings)))
    return E.pbf(blocks, compression="zlib")


def slice_stream(stream, sizes):
    position = 0
    index = 0
    while position < len(stream):
        size = sizes[index % len(sizes)]
        yield stream[position:position + size]
        position += size
        index += 1


def show(roads):
    for road in roads:
        print("way %d  tags=%r" % (road["id"], road["tags"]))
        print("  refs  =", road["refs"])
        print("  coords=", road["coords"])


def main():
    workdir = tempfile.mkdtemp()
    try:
        stream = build_stream()
        print("PBF stream: %d bytes, ways encoded before the nodes they use"
              % len(stream))
        print("round-trip encoder: tests/pbf_encode.py (independent of osm.py)")

        chunks = slice_stream(stream, [1, 2, 3, 5, 8, 13, 21, 34])
        emitted = []
        iterator = osm.roads(chunks, workdir)
        try:
            for road in iterator:
                emitted.append(road)
        finally:
            iterator.close()

        show(emitted)
        assert emitted == EXPECTED, "decoded roads differ from the scenario"
        print("\nOK: %d roads match the independently encoded scenario"
              % len(emitted))
        assert os.listdir(workdir) == [], "temporary files were not cleaned up"
        print("OK: workdir is empty after successful iteration")

        # Early close must also clean up: start one road, then stop.
        iterator = osm.roads(build_stream(), workdir)
        first = next(iterator)
        print("\nearly close after first road: way %d; open temp files: %d"
              % (first["id"], len(os.listdir(workdir))))
        iterator.close()
        assert os.listdir(workdir) == [], "temporary files remain after close()"
        print("OK: workdir is empty after early close()")

        # A missing node is detected before anything is produced.
        try:
            list(osm.roads(build_stream(late_nodes=False), workdir))
        except ValueError as error:
            print("\nmissing-node file rejected before output:", error)
        else:
            raise AssertionError("missing node was not rejected")
        assert os.listdir(workdir) == [], "temporary files remain after error"
        print("OK: workdir is empty after the ValueError")
        print("\ndemo complete")
    finally:
        os.rmdir(workdir)


if __name__ == "__main__":
    main()
