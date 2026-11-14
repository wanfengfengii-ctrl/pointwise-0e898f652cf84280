"""Independent OSM PBF 0.6 encoder used only by the tests and demo.

It deliberately shares no code with ``osm.py``: protobuf framing, varints,
two's-complement/zig-zag helpers, string tables, DenseNodes deltas and file
framing are written from the OSM PBF specification so that the decoder can be
checked against a second, independently written implementation.
"""

import struct
import zlib


# ---------------------------------------------------------------- protobuf


def varint(value):
    if value < 0:
        raise ValueError("varint requires a non-negative value")
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def twice(value):
    """Zig-zag encoding."""
    return (value << 1) ^ (value >> 63) if value < 0 else value << 1


def tag(field, wire):
    return varint((field << 3) | wire)


def f_varint(field, value):
    return tag(field, 0) + varint(value & 0xFFFFFFFFFFFFFFFF)


def f_bytes(field, payload):
    if isinstance(payload, str):
        payload = payload.encode("utf-8")
    return tag(field, 2) + varint(len(payload)) + payload


def repeated(field, values, packed=True, zig=False):
    if packed:
        body = b"".join(varint(twice(v) if zig else v) for v in values)
        return f_bytes(field, body)
    return b"".join(f_varint(field, twice(v) if zig else v) for v in values)


# ----------------------------------------------------------------- messages


def string_table(strings):
    return b"".join(f_bytes(1, s) for s in strings)


def dense_nodes(nodes, index, packed=True, dense_info=None):
    """Encode DenseNodes.

    ``nodes`` is an iterable of ``(id, lon_nano, lat_nano, tags)`` where raw
    granularity/offset reversal is unnecessary because the caller passes
    already-reversed raw values via :func:`dense_raw`.
    """
    ids, lats, lons, kvs = [], [], [], []
    prev_id = prev_lat = prev_lon = 0
    for node_id, raw_lon, raw_lat, tags in nodes:
        ids.append(node_id - prev_id)
        lats.append(raw_lat - prev_lat)
        lons.append(raw_lon - prev_lon)
        prev_id, prev_lat, prev_lon = node_id, raw_lat, raw_lon
        for key, value in tags:
            kvs.extend((index(key), index(value)))
        kvs.append(0)
    body = repeated(1, ids, packed, zig=True)
    body += repeated(8, lats, packed, zig=True)
    body += repeated(9, lons, packed, zig=True)
    if kvs:
        body += repeated(10, kvs, packed)
    if dense_info is not None:
        body += f_bytes(5, dense_info)
    return body


def way(way_id, tags, refs, index, packed=True):
    body = f_varint(1, way_id)
    body += repeated(2, [index(k) for k, _ in tags], packed)
    body += repeated(3, [index(v) for _, v in tags], packed)
    deltas = []
    previous = 0
    for ref in refs:
        deltas.append(ref - previous)
        previous = ref
    body += repeated(8, deltas, packed, zig=True)
    return body


def primitive_block(groups, strings=(), granularity=100, lat_offset=0,
                    lon_offset=0, date_granularity=1000):
    body = f_bytes(1, string_table(strings))
    for group in groups:
        body += f_bytes(2, group)
    if granularity != 100:
        body += f_varint(17, granularity)
    if date_granularity != 1000:
        body += f_varint(18, date_granularity)
    if lat_offset:
        body += f_varint(19, lat_offset)
    if lon_offset:
        body += f_varint(20, lon_offset)
    return body


def group(ways=(), dense=None):
    body = b""
    if dense is not None:
        body += f_bytes(2, dense)
    for item in ways:
        body += f_bytes(3, item)
    return body


# ------------------------------------------------------------------- blobs


def blob_header(blob_type, datasize):
    return f_bytes(1, blob_type) + f_varint(3, datasize)


def blob_raw(payload, raw_size=True):
    body = f_bytes(1, payload)
    if raw_size:
        body += f_varint(2, len(payload))
    return body


def blob_zlib(payload, raw_size=True):
    body = f_bytes(3, zlib.compress(payload))
    if raw_size:
        body += f_varint(2, len(payload))
    return body


def frame(blob_type, payload, compression="zlib"):
    if compression == "zlib":
        blob = blob_zlib(payload)
    elif compression == "raw":
        blob = blob_raw(payload)
    else:
        raise ValueError("unknown compression")
    header = blob_header(blob_type, len(blob))
    return struct.pack(">I", len(header)) + header + blob


def header_block(features=("OsmSchema-V0.6", "DenseNodes")):
    return b"".join(f_bytes(4, feature) for feature in features)


def pbf(blocks, compression="zlib"):
    return b"".join(frame(kind, payload, compression) for kind, payload in blocks)


# --------------------------------------------------------------- scenarios


class Encoder:
    """Convenience encoder producing a conventional PBF byte string."""

    def __init__(self, extra_strings=()):
        self.strings = [""]
        self.lookup = {"": 0}
        for value in extra_strings:
            self.use(value)

    def use(self, value):
        if value not in self.lookup:
            self.lookup[value] = len(self.strings)
            self.strings.append(value)
        return self.lookup[value]

    def index(self, value):
        return self.use(value)

    def dense(self, nodes, packed=True, granularity=100, lat_offset=0,
              lon_offset=0):
        raw = []
        for node_id, lon_nano, lat_nano, tags in nodes:
            dlon = lon_nano - lon_offset
            dlat = lat_nano - lat_offset
            if dlon % granularity or dlat % granularity:
                raise AssertionError("node does not fit granularity/offset")
            raw.append(
                (node_id, dlon // granularity, dlat // granularity,
                 [(self.use(k), self.use(v)) for k, v in tags])
            )
        return dense_nodes(raw, lambda value: value, packed=packed)

    def way(self, way_id, tags, refs, packed=True):
        return way(
            way_id,
            [(self.use(k), self.use(v)) for k, v in tags],
            refs,
            lambda value: value,
            packed=packed,
        )

    def block(self, group_bytes, granularity=100, lat_offset=0, lon_offset=0):
        return primitive_block(
            [group_bytes], self.strings, granularity, lat_offset, lon_offset
        )
