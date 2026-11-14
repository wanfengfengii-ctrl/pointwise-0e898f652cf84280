"""Contract-independent verification of ``osm.roads``.

Everything here is checked against ``tests/pbf_encode.py``, a second
implementation of OSM PBF 0.6 encoding that shares no code with ``osm.py``.
The script is deterministic and needs no network or third-party packages.

Run from the repository root:

    python3 verify.py
"""

import os
import random
import struct
import sys
import tempfile

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tests"))

import osm  # noqa: E402
import pbf_encode as E  # noqa: E402

FAILURES = []


def check(condition, message):
    if condition:
        print("  ok   %s" % message)
    else:
        FAILURES.append(message)
        print("  FAIL %s" % message)


def workdir():
    return tempfile.mkdtemp()


def decode(stream, slices=None):
    """Decode with strict temp-file cleanup on every exit path."""
    directory = workdir()
    try:
        if slices:
            chunks = [stream[i:i + slices] for i in range(0, len(stream), slices)]
        else:
            chunks = stream
        return list(osm.roads(chunks, directory))
    finally:
        check(os.listdir(directory) == [], "no temporary files left in workdir")
        os.rmdir(directory)


def blob_boundaries(stream):
    """Offsets at which each Blob frame ends, via osm's own header parser."""
    offsets = set()
    position = 0
    while position < len(stream):
        length = struct.unpack(">I", stream[position:position + 4])[0]
        kind, datasize = osm._parse_blob_header(
            stream[position + 4:position + 4 + length]
        )
        position += 4 + length + datasize
        offsets.add(position)
    return offsets


def scenario():
    """Ways first, referenced nodes in later Blobs; mixed way types."""
    encoder = E.Encoder()
    ways = [
        (1, [("highway", "primary"), ("name", "First")], [10, 11, 10, 12]),
        (2, [("building", "yes")], [10, 11]),
        (3, [("highway", "service")], [11, 13]),
        (4, [("highway", "path")], []),
    ]
    way_group = b"".join(
        E.f_bytes(3, encoder.way(way_id, tags, refs))
        for way_id, tags, refs in ways
    )
    node_groups = [
        [(10, 400_000_000, 100_000_000, []),
         (11, 400_000_500, 100_000_300, [("name", "junction")])],
        [(12, 400_000_900, 100_000_700, []),
         (13, 400_000_300, 100_000_600, [])],
    ]
    blocks = [("OSMHeader", E.header_block()),
              ("OSMData", E.primitive_block([way_group], encoder.strings))]
    for nodes in node_groups:
        blocks.append((
            "OSMData",
            E.primitive_block([E.f_bytes(2, encoder.dense(nodes))],
                              encoder.strings),
        ))
    return E.pbf(blocks), ways, {node[0]: node for group in node_groups
                                 for node in group}


def main():
    print("osm.roads contract verification")

    print("\n[1] late nodes, source order, refs/coords fidelity")
    stream, ways, nodes = scenario()
    roads = decode(stream)
    expected_ids = [way_id for way_id, tags, _ in ways if "highway" in dict(tags)]
    check([road["id"] for road in roads] == expected_ids,
          "only highway Ways, in source order: %r" % expected_ids)
    for road in roads:
        source = next(w for w in ways if w[0] == road["id"])
        check(road["refs"] == source[2], "way %d keeps its refs" % road["id"])
        check([list(nodes[ref][1:3]) for ref in road["refs"]] == road["coords"],
              "way %d coords follow the node table" % road["id"])
    check(roads[0]["refs"] == [10, 11, 10, 12],
          "repeated refs are preserved in way 1")

    print("\n[2] arbitrary chunk boundaries")
    for size in (None, 1, 2, 3, 7, 16, 61, 4096):
        got = decode(stream, None if size is None else size)
        check(got == roads, "identical output with chunk size %r" % (size,))

    print("\n[3] truncation at every length raises ValueError")
    wd = workdir()
    boundaries = blob_boundaries(stream)
    try:
        for length in range(len(stream)):
            try:
                result = list(osm.roads(stream[:length], wd))
            except ValueError:
                result = None
            if length in boundaries:
                # A Blob-aligned prefix is structurally valid; it can still
                # fail reference validation because later Blobs are missing.
                check(result is None or all(
                    len(road["coords"]) == len(road["refs"]) for road in result),
                    "Blob-aligned prefix %d is structurally accepted" % length)
            else:
                check(result is None,
                      "truncation at %d rejected" % length)
            check(os.listdir(wd) == [], "cleanup after truncation at %d" % length)
    finally:
        os.rmdir(wd)

    print("\n[4] single-byte mutation never escapes ValueError")
    wd = workdir()
    try:
        for position in range(0, len(stream), 3):
            mutated = (stream[:position]
                       + bytes([stream[position] ^ 0xA5])
                       + stream[position + 1:])
            try:
                list(osm.roads(mutated, wd))
            except ValueError:
                pass
            except Exception as error:  # pragma: no cover
                FAILURES.append("mutation at %d raised %r" % (position, error))
                print("  FAIL mutation at %d raised %r" % (position, error))
            check(os.listdir(wd) == [], "cleanup after mutation at %d" % position)
    finally:
        os.rmdir(wd)
    print("  (no non-ValueError exceptions)" if not FAILURES else "")

    print("\n[5] missing reference is an error, produced after full validation")
    enc = E.Encoder()
    ways_group = E.f_bytes(3, enc.way(5, [("highway", "a")], [10, 999]))
    good_group = E.f_bytes(2, enc.dense([(10, 0, 0, [])]))
    data = E.pbf([("OSMHeader", E.header_block()),
                  ("OSMData", E.primitive_block([good_group], enc.strings)),
                  ("OSMData", E.primitive_block([ways_group], enc.strings))])
    wd = workdir()
    try:
        produced = []
        try:
            for road in osm.roads(data, wd):
                produced.append(road["id"])
        except ValueError as error:
            check("999" in str(error), "missing node reported: %s" % error)
        check(produced == [],
              "no road emitted before reference validation completes")
    finally:
        check(os.listdir(wd) == [], "cleanup after missing-node error")
        os.rmdir(wd)

    print("\n[6] compression variants and unpacked fields")
    enc = E.Encoder()
    group = E.f_bytes(2, enc.dense([(1, 0, 0, []), (2, 500, 500, [])], packed=False))
    group += E.f_bytes(3, enc.way(7, [("highway", "a")], [1, 2, 1], packed=False))
    block = E.primitive_block([group], enc.strings)
    for compression in ("raw", "zlib"):
        data = E.pbf([("OSMHeader", E.header_block()), ("OSMData", block)],
                     compression=compression)
        expected = [{"id": 7, "tags": {"highway": "a"}, "refs": [1, 2, 1],
                     "coords": [[0, 0], [500, 500], [0, 0]]}]
        check(decode(data) == expected, "%s Blob with unpacked fields" % compression)

    print("\n[7] header capability requirements")
    enc = E.Encoder()
    block = E.primitive_block(
        [E.f_bytes(2, enc.dense([(1, 0, 0, [])]))], enc.strings)
    for features, expected_ok in (
        (("OsmSchema-V0.6", "DenseNodes"), True),
        (("OsmSchema-V0.6",), False),
        (("DenseNodes",), False),
        (("OsmSchema-V0.6", "DenseNodes", "HistoricalInformation"), False),
    ):
        data = E.pbf([("OSMHeader", E.header_block(features)),
                      ("OSMData", block)])
        wd = workdir()
        try:
            try:
                list(osm.roads(data, wd))
                ok = True
            except ValueError:
                ok = False
        finally:
            check(os.listdir(wd) == [], "cleanup for header %r" % (features,))
            os.rmdir(wd)
        check(ok == expected_ok, "header features %r accepted=%s"
              % (features, expected_ok))

    print("\n[8] source exceptions propagate unchanged")
    class SourceError(RuntimeError):
        pass

    def failing():
        yield stream[:5]
        raise SourceError("boom")

    wd = workdir()
    try:
        try:
            list(osm.roads(failing(), wd))
        except SourceError as error:
            check(str(error) == "boom", "source error surfaced unchanged")
    finally:
        check(os.listdir(wd) == [], "cleanup after source error")
        os.rmdir(wd)

    print("\n[9] close() before and during iteration cleans up")
    wd = workdir()
    try:
        iterator = osm.roads(stream, wd)
        first = next(iterator)
        check(first["id"] == roads[0]["id"], "iteration starts with the first Way")
        check(os.listdir(wd) != [], "workdir is used during iteration")
        iterator.close()
        check(os.listdir(wd) == [], "close() mid-iteration removes all files")
        iterator = osm.roads(stream, wd)
        iterator.close()
        check(os.listdir(wd) == [], "close() before iteration leaves nothing")
    finally:
        os.rmdir(wd)

    print("\n[10] granularity and offsets")
    lon0, lat0 = 100_000_000, 50_000_000
    enc = E.Encoder()
    group = E.f_bytes(2, enc.dense(
        [(1, lon0, lat0, []), (2, lon0 + 500 * 7, lat0 + 700 * 7, [])],
        granularity=7, lat_offset=lat0, lon_offset=lon0))
    group += E.f_bytes(3, enc.way(1, [("highway", "a")], [1, 2]))
    data = E.pbf([("OSMHeader", E.header_block()),
                  ("OSMData", E.primitive_block([group], enc.strings,
                                                granularity=7,
                                                lat_offset=lat0,
                                                lon_offset=lon0))])
    check(decode(data)[0]["coords"] == [[lon0, lat0],
                                        [lon0 + 3500, lat0 + 4900]],
          "granularity/offset scaling")

    print("\n[11] randomized round trips")
    random.seed(20261114)
    for trial in range(25):
        enc = E.Encoder()
        ids = random.sample(range(-2 ** 40, 2 ** 40), random.randint(1, 40))
        nodes = [(node_id, random.randint(-2 ** 39, 2 ** 39) * 100,
                  random.randint(-2 ** 39, 2 ** 39) * 100, []) for node_id in ids]
        ways = []
        used = set()
        for _ in range(random.randint(1, 12)):
            way_id = random.randint(0, 2 ** 40)
            if way_id in used:
                continue
            used.add(way_id)
            tags = [("highway", random.choice(["a", "b"]))] \
                if random.random() < 0.6 else [("x", "y")]
            refs = [random.choice(ids) for _ in range(random.randint(0, 6))]
            ways.append((way_id, tags, refs))
        group = E.f_bytes(2, enc.dense(nodes))
        group += b"".join(E.f_bytes(3, enc.way(*item)) for item in ways)
        data = E.pbf([("OSMHeader", E.header_block()),
                      ("OSMData", E.primitive_block([group], enc.strings))])
        got = decode(data, random.choice([None, 2, 5, 13, 100]))
        wanted = [way for way in ways if "highway" in dict(way[1])]
        check([road["id"] for road in got] == [way[0] for way in wanted]
              and all(road["refs"] == way[2] for road, way in zip(got, wanted)),
              "random trial %d" % trial)

    print()
    if FAILURES:
        print("%d FAILURE(S)" % len(FAILURES))
        return 1
    print("all verification checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
