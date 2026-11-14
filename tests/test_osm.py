"""Tests for osm.roads against an independently written PBF encoder."""

import gc
import os
import struct
import sys
import tempfile
import unittest
import zlib

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import osm  # noqa: E402
import pbf_encode as E  # noqa: E402


def make_pbf(content, compression="zlib", packed=True, order=("header", "data")):
    """Encode content = list of dicts; returns bytes."""
    enc = E.Encoder()
    blocks = []
    header = E.header_block()
    data_blocks = []
    for spec in content:
        strings_before = list(enc.strings)
        ways = []
        dense = None
        if spec.get("nodes") is not None:
            dense = enc.dense(
                spec["nodes"],
                packed=packed,
                granularity=spec.get("granularity", 100),
                lat_offset=spec.get("lat_offset", 0),
                lon_offset=spec.get("lon_offset", 0),
            )
        for way_id, tags, refs in spec.get("ways", ()):
            ways.append(enc.way(way_id, tags, refs, packed=packed))
        grp = b""
        if dense is not None:
            grp += E.f_bytes(2, dense)
        for item in ways:
            grp += E.f_bytes(3, item)
        if spec.get("group") is not None:
            grp += spec["group"]
        block = E.primitive_block(
            [grp],
            enc.strings,
            granularity=spec.get("granularity", 100),
            lat_offset=spec.get("lat_offset", 0),
            lon_offset=spec.get("lon_offset", 0),
        )
        assert enc.strings[:len(strings_before)] == strings_before
        data_blocks.append(("OSMData", block))
    if order == ("header", "data"):
        blocks = [("OSMHeader", header)] + data_blocks
    else:
        blocks = data_blocks + [("OSMHeader", header)]
    return E.pbf(blocks, compression=compression)


class Chunking:
    def __init__(self, data, size):
        self._chunks = [data[i:i + size] for i in range(0, len(data), size)]

    def __iter__(self):
        return iter(self._chunks)


class Base(unittest.TestCase):
    def setUp(self):
        self.workdir = tempfile.mkdtemp()

    def tearDown(self):
        leftover = os.listdir(self.workdir)
        self.assertEqual(leftover, [], "temporary files left behind: %r" % leftover)
        os.rmdir(self.workdir)

    def collect(self, data, chunk=None):
        chunks = data if chunk is None else Chunking(data, chunk)
        return list(osm.roads(chunks, self.workdir))


class TestCorrectness(Base):
    def simple(self):
        nodes = [
            (1, 1000000000, 2000000000, []),
            (2, 1000100000, 2000200000, [("amenity", "bench")]),
            (3, 1000200000, 2000400000, []),
            (-5, 1000300000, 2000600000, []),
        ]
        ways = [
            (10, [("highway", "residential"), ("name", "Main")], [1, 2, 3, 2, -5]),
            (11, [("railway", "custom")], [1, 2]),
            (12, [("name", "A")], [-5, 1]),
            (13, [("highway", "service")], [3, 3]),
        ]
        return make_pbf([{"nodes": nodes, "ways": ways}])

    def test_basic(self):
        roads = self.collect(self.simple())
        self.assertEqual([r["id"] for r in roads], [10, 13])
        first = roads[0]
        self.assertEqual(first["tags"], {"highway": "residential", "name": "Main"})
        self.assertEqual(first["refs"], [1, 2, 3, 2, -5])
        self.assertEqual(
            first["coords"],
            [
                [1000000000, 2000000000],
                [1000100000, 2000200000],
                [1000200000, 2000400000],
                [1000100000, 2000200000],
                [1000300000, 2000600000],
            ],
        )
        self.assertEqual(roads[1]["refs"], [3, 3])
        self.assertEqual(roads[1]["coords"], [[1000200000, 2000400000]] * 2)

    def test_unpacked_fields(self):
        roads = self.collect(self.simple(), chunk=3)
        self.assertEqual([r["id"] for r in roads], [10, 13])

    def test_raw_blobs(self):
        roads = self.collect(self.simple(), chunk=1)
        self.assertEqual([r["id"] for r in roads], [10, 13])

    def test_raw_and_chunked(self):
        data = make_pbf([{"nodes": [(1, 0, 0, [])],
                         "ways": [(7, [("highway", "x")], [1])]}],
                        compression="raw", packed=False)
        roads = self.collect(data, chunk=1)
        self.assertEqual(roads, [{"id": 7, "tags": {"highway": "x"},
                                   "refs": [1], "coords": [[0, 0]]}])

    def test_granularity_and_offsets(self):
        lon0, lat0 = -987654321, 12345
        spec = {
            "nodes": [(50, lon0 + 700, lat0 + 1400, []),
                      (51, lon0 + 2800, lat0 + 5600, [])],
            "ways": [(1, [("highway", "a")], [50, 51])],
            "granularity": 7,
            "lat_offset": lat0,
            "lon_offset": lon0,
        }
        roads = self.collect(make_pbf([spec]))
        self.assertEqual(
            roads[0]["coords"],
            [[lon0 + 700, lat0 + 1400], [lon0 + 2800, lat0 + 5600]],
        )

    def test_negative_and_zero_deltas(self):
        spec = {
            "nodes": [(100, 0, 0, []), (-100, 500, -500, []), (3, -500, 500, [])],
            "ways": [(1, [("highway", "a")], [100, -100, 3])],
        }
        roads = self.collect(make_pbf([spec]))
        self.assertEqual(roads[0]["coords"],
                         [[0, 0], [500, -500], [-500, 500]])

    def test_ways_before_nodes_later_block(self):
        nodes_spec = {"nodes": [(1, 0, 0, []), (2, 100, 0, []), (3, 200, 0, []),
                                (4, 400, 400, [])]}
        ways_spec = {"ways": [
            (9, [("highway", "primary")], [4, 3, 2, 1]),
        ]}
        roads = self.collect(make_pbf([ways_spec, nodes_spec]))
        self.assertEqual([r["id"] for r in roads], [9])
        self.assertEqual(roads[0]["coords"],
                         [[400, 400], [200, 0], [100, 0], [0, 0]])

    def test_source_way_order_across_blocks(self):
        specs = [
            {"nodes": [(1, 0, 0, [])],
             "ways": [(30, [("highway", "a")], [1])]},
            {"nodes": [(2, 1000, 0, [])],
             "ways": [(29, [("highway", "b")], [2])]},
            {"ways": [(31, [("highway", "c")], [1, 2])]},
        ]
        roads = self.collect(make_pbf(specs))
        self.assertEqual([r["id"] for r in roads], [30, 29, 31])

    def test_empty_refs(self):
        spec = {"nodes": [(1, 0, 0, [])],
                "ways": [(1, [("highway", "a")], [])]}
        roads = self.collect(make_pbf([spec]))
        self.assertEqual(roads, [{"id": 1, "tags": {"highway": "a"},
                                  "refs": [], "coords": []}])

    def test_string_table_and_unicode(self):
        spec = {
            "nodes": [(1, 0, 0, [("name", "节点")])],
            "ways": [(1, [("highway", "residential"),
                          ("name", "主街"), ("note", "")], [1])],
        }
        roads = self.collect(make_pbf([spec]))
        self.assertEqual(roads[0]["tags"]["name"], "主街")
        self.assertEqual(roads[0]["tags"]["note"], "")

    def test_multiple_primitive_groups(self):
        spec = {"nodes": [(1, 0, 0, [])],
                "ways": [(1, [("highway", "a")], [1])],
                "group": E.group(ways=[E.f_varint(1, 999)])}
        # The extra group (a Way without tags) is valid PBF, not a road.
        roads = self.collect(make_pbf([spec]))
        self.assertEqual([r["id"] for r in roads], [1])

    def test_duplicate_refs_preserved(self):
        spec = {"nodes": [(1, 0, 0, []), (2, 500, 0, [])],
                "ways": [(5, [("highway", "a")], [1, 1, 2, 1, 2, 2])]}
        roads = self.collect(make_pbf([spec]))
        self.assertEqual(roads[0]["refs"], [1, 1, 2, 1, 2, 2])
        self.assertEqual(roads[0]["coords"],
                         [[0, 0], [0, 0], [500, 0], [0, 0], [500, 0], [500, 0]])

    def test_file_object_source(self):
        import io
        data = self.simple()
        roads = list(osm.roads(io.BytesIO(data), self.workdir))
        self.assertEqual([r["id"] for r in roads], [10, 13])

    def test_single_bytes_source(self):
        roads = list(osm.roads(self.simple(), self.workdir))
        self.assertEqual([r["id"] for r in roads], [10, 13])


class TestValidation(Base):
    def assertValueError(self, data, message=None):
        with self.assertRaises(ValueError) as caught:
            list(osm.roads(data, self.workdir))
        if message:
            self.assertIn(message, str(caught.exception))

    def test_missing_node(self):
        spec = {"nodes": [(1, 0, 0, [])],
                "ways": [(1, [("highway", "a")], [1, 2])]}
        self.assertValueError(make_pbf([spec]), "missing node")

    def test_header_features_missing(self):
        enc = E.Encoder()
        spec_nodes = enc.dense([(1, 0, 0, [])])
        data_block = enc.block(E.f_bytes(2, spec_nodes))
        for features in (("OsmSchema-V0.6",), ("DenseNodes",), ()):
            data = E.pbf([("OSMHeader", E.header_block(features)),
                          ("OSMData", data_block)])
            self.assertValueError(data, "header")

    def test_header_unknown_feature(self):
        enc = E.Encoder()
        data_block = enc.block(E.group(dense=enc.dense([(1, 0, 0, [])])))
        data = E.pbf([("OSMHeader", E.header_block(
            ("OsmSchema-V0.6", "DenseNodes", "LocationsOnWays"))),
            ("OSMData", data_block)])
        self.assertValueError(data, "feature")

    def test_no_header(self):
        enc = E.Encoder()
        data_block = enc.block(E.group(dense=enc.dense([(1, 0, 0, [])])))
        self.assertValueError(E.pbf([("OSMData", data_block)]), "OSMHeader")

    def test_truncated_stream(self):
        data = make_pbf([{"nodes": [(1, 0, 0, [])],
                          "ways": [(1, [("highway", "a")], [1])]}])
        for cut in (1, 5, 40, len(data) - 1):
            self.assertValueError(data[:cut])

    def test_bad_zlib(self):
        enc = E.Encoder()
        block = enc.block(E.group(dense=enc.dense([(1, 0, 0, [])])))
        good = E.pbf([("OSMHeader", E.header_block()), ("OSMData", block)])
        # Corrupt a byte inside the compressed data of the data blob.
        header_size = struct.unpack(">I", good[:4])[0]
        start = 4 + header_size
        blob_header_size = struct.unpack(">I", good[start:start + 4])[0]
        pos = start + 4 + blob_header_size
        for offset in range(pos + 5, len(good) - 4):
            mutated = good[:offset] + bytes([good[offset] ^ 0xFF]) + good[offset + 1:]
            with self.assertRaises(ValueError):
                list(osm.roads(mutated, self.workdir))
            break

    def test_bad_string_index(self):
        enc = E.Encoder()
        table = E.string_table(["", "highway"])
        raw_way = E.f_varint(1, 1) + E.repeated(2, [99], True) + E.repeated(3, [1], True)
        block = E.f_bytes(1, table) + E.f_bytes(
            2, E.f_bytes(3, raw_way)
        )
        data = E.pbf([("OSMHeader", E.header_block()), ("OSMData", block)])
        self.assertValueError(data, "index")

    def test_dense_counts_mismatch(self):
        table = E.string_table([""])
        dense = E.repeated(1, [0, 0], True, zig=True) + E.repeated(8, [0], True, zig=True)
        dense += E.repeated(9, [0], True, zig=True)
        block = E.f_bytes(1, table) + E.f_bytes(2, E.f_bytes(2, dense))
        data = E.pbf([("OSMHeader", E.header_block()), ("OSMData", block)])
        self.assertValueError(data)

    def test_dense_keys_vals_missing_separator(self):
        table = E.string_table(["", "k", "v"])
        dense = E.repeated(1, [0], True, zig=True)
        dense += E.repeated(8, [0], True, zig=True)
        dense += E.repeated(9, [0], True, zig=True)
        dense += E.repeated(10, [1, 2], True)
        block = E.f_bytes(1, table) + E.f_bytes(2, E.f_bytes(2, dense))
        data = E.pbf([("OSMHeader", E.header_block()), ("OSMData", block)])
        self.assertValueError(data)

    def test_plain_node_and_relation_supported(self):
        # Plain Node messages are valid OSM PBF and are decoded; Relations are
        # irrelevant to roads but must not break an otherwise valid file.
        enc = E.Encoder()
        enc.use("highway")
        enc.use("a")
        relation = E.f_varint(1, 5)
        node = (E.f_varint(1, E.twice(7)) + E.f_varint(8, E.twice(1000))
                + E.f_varint(9, E.twice(2000)))
        grp = (E.f_bytes(2, enc.dense([(1, 0, 0, [])]))
               + E.f_bytes(1, node)
               + E.f_bytes(4, relation)
               + E.f_bytes(3, enc.way(9, [("highway", "a")], [1, 7])))
        data = E.pbf([
            ("OSMHeader", E.header_block()),
            ("OSMData", E.primitive_block([grp], enc.strings)),
        ])
        roads = list(osm.roads(data, self.workdir))
        self.assertEqual([r["id"] for r in roads], [9])
        self.assertEqual(roads[0]["coords"], [[0, 0], [200000, 100000]])

    def test_malformed_plain_node_rejected(self):
        table = E.string_table([""])
        block = E.f_bytes(1, table) + E.f_bytes(2, E.f_bytes(1, E.f_varint(1, 0)))
        data = E.pbf([("OSMHeader", E.header_block()), ("OSMData", block)])
        with self.assertRaises(ValueError):
            list(osm.roads(data, self.workdir))

    def test_duplicate_way_id(self):
        spec = {"nodes": [(1, 0, 0, [])],
                "ways": [(1, [("highway", "a")], [1]),
                         (1, [("highway", "b")], [1])]}
        self.assertValueError(make_pbf([spec]))

    def test_way_key_val_count_mismatch(self):
        enc = E.Encoder()
        table = E.string_table(["", "highway"])
        raw_way = E.f_varint(1, 1) + E.repeated(2, [1, 1], True) + E.repeated(3, [1], True)
        block = E.f_bytes(1, table) + E.f_bytes(2, E.f_bytes(3, raw_way))
        data = E.pbf([("OSMHeader", E.header_block()), ("OSMData", block)])
        self.assertValueError(data)

    def test_payload_error_detected_before_first_yield(self):
        enc = E.Encoder()
        good_block = enc.block(E.group(**{
            "dense": enc.dense([(1, 0, 0, [])]),
        }, ways=[enc.way(1, [("highway", "a")], [1])]))
        nodes = enc.dense([(2, 0, 0, [])])
        broken = b""
        blocks = [
            ("OSMHeader", E.header_block()),
            ("OSMData", good_block),
            ("OSMData", b"\xff\xff\xff"),
        ]
        iterator = osm.roads(E.pbf(blocks), self.workdir)
        with self.assertRaises(ValueError):
            next(iterator)
        del broken, nodes

    def test_blob_header_too_large(self):
        data = struct.pack(">I", 70000) + b"\x00" * 70000
        with self.assertRaises(ValueError):
            list(osm.roads(data, self.workdir))

    def test_raw_size_mismatch(self):
        payload = E.header_block()
        blob = E.f_bytes(1, payload) + E.f_varint(2, 9999)
        header = E.blob_header("OSMHeader", len(blob))
        data = struct.pack(">I", len(header)) + header + blob
        with self.assertRaises(ValueError):
            list(osm.roads(data, self.workdir))

    def test_exception_from_source_propagates_unchanged(self):
        class Boom(Exception):
            pass

        def source():
            yield b"\x00"
            raise Boom("source failure")

        iterator = osm.roads(source(), self.workdir)
        with self.assertRaises(Boom):
            list(iterator)

    def test_bad_chunk_type(self):
        with self.assertRaises(TypeError):
            list(osm.roads(iter(["not bytes"]), self.workdir))


class TestLifecycle(Base):
    def test_cleanup_on_early_close(self):
        data = make_pbf([{
            "nodes": [(1, 0, 0, []), (2, 100, 100, [])],
            "ways": [(1, [("highway", "a")], [1]), (2, [("highway", "b")], [2])],
        }])
        iterator = osm.roads(data, self.workdir)
        first = next(iterator)
        self.assertEqual(first["id"], 1)
        iterator.close()
        self.assertEqual(os.listdir(self.workdir), [])

    def test_cleanup_when_never_started(self):
        iterator = osm.roads(b"", self.workdir)
        iterator.close()
        self.assertEqual(os.listdir(self.workdir), [])

    def test_cleanup_after_error(self):
        data = make_pbf([{"nodes": [], "ways": [(1, [("highway", "a")], [1])]}])
        with self.assertRaises(ValueError):
            list(osm.roads(data, self.workdir))
        self.assertEqual(os.listdir(self.workdir), [])

    def test_cleanup_after_garbage_collection(self):
        data = make_pbf([{"nodes": [(1, 0, 0, [])],
                          "ways": [(1, [("highway", "a")], [1])]}])
        iterator = osm.roads(data, self.workdir)
        next(iterator)
        del iterator
        gc.collect()
        self.assertEqual(os.listdir(self.workdir), [])

    def test_closeable_iterable_repeatedly(self):
        data = make_pbf([{"nodes": [(1, 0, 0, [])],
                          "ways": [(1, [("highway", "a")], [1])]}])
        for _ in range(3):
            roads = list(osm.roads(data, self.workdir))
            self.assertEqual(len(roads), 1)

    def test_rejects_non_directory(self):
        path = os.path.join(self.workdir, "file")
        open(path, "wb").close()
        iterator = osm.roads(b"", path)
        with self.assertRaises(NotADirectoryError):
            list(iterator)
        os.unlink(path)


if __name__ == "__main__":
    unittest.main()
