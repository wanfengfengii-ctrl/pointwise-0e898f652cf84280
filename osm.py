"""Streaming OSM PBF 0.6 -> polyline conversion using only the standard library.

Public API::

    roads(chunks, workdir) -> closeable iterator of dicts

``chunks`` is a single-pass stream of ``bytes`` (any iterable, a bytes-like
object, or a binary file object); chunk boundaries may fall on any byte,
including inside a protobuf message or a Blob.  ``workdir`` must be an
existing directory and is used for temporary, module-owned files only.

Each produced dictionary has the shape::

    {"id": int, "tags": {str: str}, "refs": [int, ...],
     "coords": [[lon_nano, lat_nano], ...]}

Ways are produced in source order.  Only ways carrying a ``highway`` tag are
produced, but every Way and DenseNodes message in the stream is decoded and
validated.  ``refs`` keeps the source node sequence, including repeats, and
``coords`` has one ``[longitude, latitude]`` pair of integer nanodegrees per
reference.

Nothing is yielded until the complete stream has been decoded and every
reference of every retained Way has been confirmed to exist, so nodes may
legitimately arrive in Blobs after the Ways that reference them.

Malformed input (truncation, bad framing/lengths/indices, broken compression,
missing nodes, ...) raises :class:`ValueError`.  Exceptions raised by the
``chunks`` source are propagated unchanged.  All files created by this module
are removed when iteration finishes, fails, or is closed early.
"""

from __future__ import annotations

import json
import os
import sqlite3
import struct
import tempfile
import zlib

__all__ = ["roads"]

_MAX_BLOB_HEADER = 64 * 1024
_MAX_BLOB = 32 * 1024 * 1024
# The limit applies to the decoded Blob payload.  The serialised Blob message
# around it carries framing bytes plus (for zlib) a compressed representation
# that can grow slightly on incompressible data, so allow modest headroom
# before rejecting a claimed datasize outright.
_MAX_SERIALISED_BLOB = _MAX_BLOB + (1 << 16)
_REQUIRED_FEATURES = frozenset(("OsmSchema-V0.6", "DenseNodes"))
_I64_MIN = -(1 << 63)
_I64_MAX = (1 << 63) - 1
_I64_SPAN = 1 << 64
_U64_MAX = (1 << 64) - 1
_NODE_BATCH = 4096
_LOOKUP_BATCH = 500


# ---------------------------------------------------------------------------
# Single-pass byte source


def _chunk_source(chunks):
    """Turn the caller's stream into an iterator of non-empty byte strings."""
    if isinstance(chunks, (bytes, bytearray, memoryview)):
        chunks = (bytes(chunks),)
    if hasattr(chunks, "read"):
        def from_file():
            while True:
                block = chunks.read(64 * 1024)
                if not block:
                    return
                yield block

        return from_file()
    return iter(chunks)


class _ByteStream:
    """Exact-length reader over a single-pass sequence of byte chunks."""

    __slots__ = ("_it", "_buf", "_pos", "_eof")

    def __init__(self, chunks):
        self._it = _chunk_source(chunks)
        self._buf = b""
        self._pos = 0
        self._eof = False

    def _fill(self):
        """Ensure at least one unread byte is buffered; False at clean EOF."""
        while self._pos >= len(self._buf):
            if self._eof:
                return False
            try:
                block = next(self._it)
            except StopIteration:
                self._eof = True
                return False
            if not isinstance(block, (bytes, bytearray, memoryview)):
                raise TypeError("chunks must yield bytes")
            block = bytes(block)
            if not block:
                continue
            self._buf = block
            self._pos = 0
        return True

    def at_eof(self):
        if self._pos < len(self._buf):
            return False
        return not self._fill()

    def read(self, size):
        """Read exactly *size* bytes or raise ValueError on truncation.

        The destination is preallocated, so memory stays O(size) even when the
        source yields many tiny chunks (for example one byte at a time).
        """
        if size < 0:
            raise ValueError("negative read length")
        if size == 0:
            return b""
        out = bytearray(size)
        filled = 0
        while filled < size:
            if self._pos >= len(self._buf) and not self._fill():
                raise ValueError("truncated PBF stream")
            take = min(size - filled, len(self._buf) - self._pos)
            out[filled:filled + take] = self._buf[self._pos:self._pos + take]
            self._pos += take
            filled += take
        return out


# ---------------------------------------------------------------------------
# Protobuf wire decoding


def _varint(data, pos, end):
    result = 0
    shift = 0
    for _ in range(10):
        if pos >= end:
            raise ValueError("truncated protobuf varint")
        byte = data[pos]
        pos += 1
        result |= (byte & 0x7f) << shift
        if not (byte & 0x80):
            if result > _U64_MAX:
                raise ValueError("protobuf varint exceeds 64 bits")
            return result, pos
        shift += 7
    raise ValueError("protobuf varint is too long")


def _fields(data):
    """Yield ``(field_number, wire_type, value)`` for a protobuf message.

    Length-delimited values are zero-copy memoryview slices of *data*.
    """
    if not isinstance(data, memoryview):
        data = memoryview(data)
    pos = 0
    end = len(data)
    while pos < end:
        tag, pos = _varint(data, pos, end)
        field = tag >> 3
        wire = tag & 7
        if field == 0:
            raise ValueError("protobuf field number 0")
        if wire == 0:
            value, pos = _varint(data, pos, end)
        elif wire == 1:
            if end - pos < 8:
                raise ValueError("truncated protobuf fixed64 field")
            value = data[pos:pos + 8]
            pos += 8
        elif wire == 2:
            length, pos = _varint(data, pos, end)
            if length > end - pos:
                raise ValueError("protobuf length exceeds message")
            value = data[pos:pos + length]
            pos += length
        elif wire == 5:
            if end - pos < 4:
                raise ValueError("truncated protobuf fixed32 field")
            value = data[pos:pos + 4]
            pos += 4
        elif wire in (3, 4):
            # Deprecated groups: fail cleanly rather than mis-frame the rest.
            raise ValueError("unsupported protobuf group wire type")
        else:
            raise ValueError("unsupported protobuf wire type %d" % wire)
        yield field, wire, value


def _signed64(value):
    return value - _I64_SPAN if value >= (1 << 63) else value


def _signed32(value):
    value &= 0xffffffff
    return value - (1 << 32) if value >= (1 << 31) else value


def _zigzag(value):
    return (value >> 1) ^ -(value & 1)


def _checked(value, what):
    if value < _I64_MIN or value > _I64_MAX:
        raise ValueError("%s is outside the int64 range" % what)
    return value


def _text(value, what):
    try:
        return bytes(value).decode("utf-8")
    except UnicodeDecodeError:
        raise ValueError("%s is not valid UTF-8" % what) from None


def _iter_field(data, field, wires, convert=None):
    """Lazily yield the values of one repeated numeric field.

    Each call rescans *data* (cheap, zero-copy memoryview slicing) and keeps
    no per-element state, so even a pathological unpacked encoding costs only
    O(one Blob) memory.  Allowed wire types are enforced per occurrence.
    """
    for number, wire, value in _fields(data):
        if number != field:
            continue
        if wire not in wires:
            raise ValueError("invalid wire type for field %d" % field)
        if wire == 0:
            yield value if convert is None else convert(value)
        else:
            pos = 0
            end = len(value)
            while pos < end:
                item, pos = _varint(value, pos, end)
                yield item if convert is None else convert(item)


def _field_has_values(data, field):
    """True when *field* occurs with at least one numeric element."""
    for number, wire, value in _fields(data):
        if number != field:
            continue
        if wire == 0 or len(value) != 0:
            return True
    return False


def _uint32(value, what):
    if value > 0xFFFFFFFF:
        raise ValueError("%s is outside the uint32 range" % what)
    return value


def _next_or_none(iterator):
    try:
        return next(iterator)
    except StopIteration:
        return None


def _string_at(strings, index, what):
    if index < 0 or index >= len(strings):
        raise ValueError("invalid %s string-table index %d" % (what, index))
    return _text(strings[index], what)


def _check_index(strings, index, what):
    if index < 0 or index >= len(strings):
        raise ValueError("invalid %s string-table index %d" % (what, index))


# ---------------------------------------------------------------------------
# Message decoding


def _parse_header_block(data):
    required = []
    for field, wire, value in _fields(data):
        if field == 4:  # required_features
            if wire != 2:
                raise ValueError("invalid required_features wire type")
            required.append(_text(value, "header feature"))
    for feature in required:
        if feature not in _REQUIRED_FEATURES:
            raise ValueError("unsupported required feature: %s" % feature)
    if "OsmSchema-V0.6" not in required or "DenseNodes" not in required:
        raise ValueError("header lacks required OSM schema/dense-node support")


def _parse_string_table(data):
    """Collect raw UTF-8 byte slices; decoding is deferred until use."""
    strings = []
    for field, wire, value in _fields(data):
        if field == 1:  # s
            if wire != 2:
                raise ValueError("invalid string-table entry")
            strings.append(value)
    return strings


class _NodeSink:
    """Buffered inserts into the on-disk node table."""

    __slots__ = ("_conn", "_rows")

    def __init__(self, conn):
        self._conn = conn
        self._rows = []

    def add(self, node_id, lon_nano, lat_nano):
        self._rows.append((node_id, _sql_value(lon_nano), _sql_value(lat_nano)))
        if len(self._rows) >= _NODE_BATCH:
            self.flush()

    def flush(self):
        if not self._rows:
            return
        try:
            self._conn.executemany(
                "INSERT INTO nodes(id, lon, lat) VALUES(?, ?, ?)", self._rows
            )
        except sqlite3.IntegrityError:
            raise ValueError("duplicate node id") from None
        self._rows = []


def _parse_dense_nodes(data, strings, granularity, lat_offset, lon_offset, sink):
    # Framing and wire types of every field in the message are checked here;
    # the arrays themselves are then streamed field by field without retaining
    # per-node Python objects.
    for field, wire, _value in _fields(data):
        if field == 5 and wire != 2:
            raise ValueError("invalid DenseInfo encoding")
        if field in (1, 8, 9, 10) and wire not in (0, 2):
            raise ValueError("invalid DenseNodes field %d encoding" % field)

    id_iter = _iter_field(data, 1, (0, 2), _zigzag)
    lat_iter = _iter_field(data, 8, (0, 2), _zigzag)
    lon_iter = _iter_field(data, 9, (0, 2), _zigzag)
    kv_iter = _iter_field(data, 10, (0, 2), _signed32)
    has_kv = _field_has_values(data, 10)

    node_id = 0
    lat_raw = 0
    lon_raw = 0
    while True:
        delta = _next_or_none(id_iter)
        if delta is None:
            break
        node_id = _checked(node_id + delta, "dense node id")
        delta = _next_or_none(lat_iter)
        if delta is None:
            raise ValueError("DenseNodes id/lat/lon counts differ")
        lat_raw = _checked(lat_raw + delta, "dense node latitude")
        delta = _next_or_none(lon_iter)
        if delta is None:
            raise ValueError("DenseNodes id/lat/lon counts differ")
        lon_raw = _checked(lon_raw + delta, "dense node longitude")

        if has_kv:
            while True:
                key = _next_or_none(kv_iter)
                if key is None:
                    raise ValueError("DenseNodes keys_vals lacks a 0 separator")
                if key == 0:
                    break
                if key < 0 or key > 0xFFFFFFFF:
                    raise ValueError("invalid dense node key index %d" % key)
                val = _next_or_none(kv_iter)
                if val is None:
                    raise ValueError("DenseNodes keys_vals lacks a value")
                if val < 0 or val > 0xFFFFFFFF:
                    raise ValueError("invalid dense node value index %d" % val)
                # Node tags are not part of the road result, but malformed
                # indices still make the block invalid.
                _check_index(strings, key, "dense node key")
                _check_index(strings, val, "dense node value")

        sink.add(
            node_id,
            lon_offset + granularity * lon_raw,
            lat_offset + granularity * lat_raw,
        )

    if _next_or_none(lat_iter) is not None or _next_or_none(lon_iter) is not None:
        raise ValueError("DenseNodes id/lat/lon counts differ")
    if _next_or_none(kv_iter) is not None:
        raise ValueError("extra DenseNodes keys_vals data")


def _parse_way(data, strings):
    for field, wire, _value in _fields(data):
        if field == 1 and wire != 0:
            raise ValueError("invalid Way id encoding")
        if field in (2, 3, 8) and wire not in (0, 2):
            raise ValueError("invalid Way field %d encoding" % field)
        if field == 4 and wire != 2:
            raise ValueError("invalid Way Info encoding")

    way_id = None
    for field, wire, value in _fields(data):
        if field == 1:
            way_id = _signed64(value)
    if way_id is None:
        raise ValueError("Way is missing its id")

    tags = {}
    key_iter = _iter_field(data, 2, (0, 2), lambda v: _uint32(v, "Way key index"))
    val_iter = _iter_field(data, 3, (0, 2), lambda v: _uint32(v, "Way value index"))
    while True:
        key_index = _next_or_none(key_iter)
        value_index = _next_or_none(val_iter)
        if key_index is None or value_index is None:
            if key_index is not None or value_index is not None:
                raise ValueError("Way keys/vals counts differ")
            break
        tags[_string_at(strings, key_index, "Way key")] = _string_at(
            strings, value_index, "Way value"
        )

    ref_list = []
    current = 0
    for delta in _iter_field(data, 8, (0, 2), _zigzag):
        current = _checked(current + delta, "Way node reference")
        ref_list.append(current)

    return way_id, tags, ref_list


def _parse_node(data, strings, granularity, lat_offset, lon_offset, sink):
    """Decode a (non-dense) OSM PBF Node message.

    The contract guarantees nodes arrive as DenseNodes, but OSM PBF 0.6 also
    allows plain Node messages in a PrimitiveGroup and they are cheap to
    support.
    """
    node_id = None
    lat = None
    lon = None
    for field, wire, value in _fields(data):
        if field == 1:  # id, sint64
            if wire != 0:
                raise ValueError("invalid Node id encoding")
            node_id = _checked(_zigzag(value), "node id")
        elif field in (2, 3):  # keys / vals, packed uint32
            if wire not in (0, 2):
                raise ValueError("invalid Node field %d encoding" % field)
        elif field == 4:  # Info
            if wire != 2:
                raise ValueError("invalid Node Info encoding")
        elif field == 8:  # lat, sint64
            if wire != 0:
                raise ValueError("invalid Node lat encoding")
            lat = _checked(_zigzag(value), "node latitude")
        elif field == 9:  # lon, sint64
            if wire != 0:
                raise ValueError("invalid Node lon encoding")
            lon = _checked(_zigzag(value), "node longitude")
    if node_id is None or lat is None or lon is None:
        raise ValueError("Node is missing id or coordinates")
    key_iter = _iter_field(data, 2, (0, 2), lambda v: _uint32(v, "Node key index"))
    val_iter = _iter_field(data, 3, (0, 2), lambda v: _uint32(v, "Node value index"))
    while True:
        key_index = _next_or_none(key_iter)
        value_index = _next_or_none(val_iter)
        if key_index is None or value_index is None:
            if key_index is not None or value_index is not None:
                raise ValueError("Node keys/vals counts differ")
            break
        _check_index(strings, key_index, "node key")
        _check_index(strings, value_index, "node value")
    sink.add(
        node_id,
        lon_offset + granularity * lon,
        lat_offset + granularity * lat,
    )


def _parse_primitive_group(data, strings, granularity, lat_offset, lon_offset,
                           sink, way_writer):
    dense = []
    nodes = []
    for field, wire, value in _fields(data):
        if field == 1:  # Node
            if wire != 2:
                raise ValueError("invalid Node encoding")
            nodes.append(value)
        elif field == 2:  # DenseNodes
            if wire != 2:
                raise ValueError("invalid DenseNodes encoding")
            dense.append(value)
        elif field == 3:  # Way
            if wire != 2:
                raise ValueError("invalid Way encoding")
            way_id, tags, refs = _parse_way(value, strings)
            if "highway" in tags:
                way_writer.write(way_id, tags, refs)
        elif field in (4, 5):  # Relation, ChangeSet
            # Not part of the road result; their framing was still checked by
            # the generic protobuf decoder above.
            if wire != 2:
                raise ValueError("invalid entity encoding")
    if len(dense) > 1:
        raise ValueError("multiple DenseNodes messages in one PrimitiveGroup")
    for node in nodes:
        _parse_node(node, strings, granularity, lat_offset, lon_offset, sink)
    if dense:
        _parse_dense_nodes(
            dense[0], strings, granularity, lat_offset, lon_offset, sink
        )


def _parse_primitive_block(data, sink, way_writer):
    string_table_data = None
    groups = []
    granularity = 100
    lat_offset = 0
    lon_offset = 0
    for field, wire, value in _fields(data):
        if field == 1:  # StringTable
            if wire != 2:
                raise ValueError("invalid StringTable encoding")
            if string_table_data is not None:
                raise ValueError("multiple StringTable messages")
            string_table_data = value
        elif field == 2:  # PrimitiveGroup
            if wire != 2:
                raise ValueError("invalid PrimitiveGroup encoding")
            groups.append(value)
        elif field == 17:  # granularity, int32
            if wire != 0:
                raise ValueError("invalid granularity encoding")
            granularity = _signed32(value)
        elif field == 18:  # date_granularity, int32
            if wire != 0:
                raise ValueError("invalid date_granularity encoding")
            _signed32(value)
        elif field == 19:  # lat_offset, int64
            if wire != 0:
                raise ValueError("invalid lat_offset encoding")
            lat_offset = _signed64(value)
        elif field == 20:  # lon_offset, int64
            if wire != 0:
                raise ValueError("invalid lon_offset encoding")
            lon_offset = _signed64(value)

    if string_table_data is None:
        raise ValueError("PrimitiveBlock is missing its StringTable")
    if granularity <= 0:
        raise ValueError("invalid granularity %d" % granularity)
    strings = _parse_string_table(string_table_data)
    for group in groups:
        _parse_primitive_group(
            group, strings, granularity, lat_offset, lon_offset, sink, way_writer
        )


def _parse_blob_header(data):
    blob_type = None
    datasize = None
    for field, wire, value in _fields(data):
        if field == 1:  # type
            if wire != 2:
                raise ValueError("invalid BlobHeader type encoding")
            blob_type = _text(value, "BlobHeader type")
        elif field == 2:  # indexdata
            if wire != 2:
                raise ValueError("invalid BlobHeader indexdata encoding")
        elif field == 3:  # datasize, int32
            if wire != 0:
                raise ValueError("invalid BlobHeader datasize encoding")
            datasize = _signed32(value)
    if blob_type is None:
        raise ValueError("BlobHeader is missing its type")
    if datasize is None:
        raise ValueError("BlobHeader is missing its datasize")
    if datasize < 0:
        raise ValueError("negative Blob datasize")
    if blob_type not in ("OSMHeader", "OSMData"):
        raise ValueError("unsupported Blob type: %s" % blob_type)
    return blob_type, datasize


def _inflate(payload):
    decompressor = zlib.decompressobj()
    try:
        # One extra byte suffices to detect an oversized expansion without
        # materialising the whole thing.
        out = decompressor.decompress(payload, _MAX_BLOB + 1)
    except zlib.error:
        raise ValueError("invalid zlib-compressed Blob") from None
    if len(out) > _MAX_BLOB or decompressor.unconsumed_tail:
        raise ValueError("Blob decompresses beyond 32 MiB")
    if not decompressor.eof:
        raise ValueError("truncated zlib-compressed Blob")
    if decompressor.unused_data:
        raise ValueError("trailing data after zlib-compressed Blob")
    try:
        tail = decompressor.flush()
    except zlib.error:
        raise ValueError("invalid zlib-compressed Blob") from None
    if tail:
        out += tail
    if len(out) > _MAX_BLOB:
        raise ValueError("Blob decompresses beyond 32 MiB")
    return out


def _parse_blob(data):
    payload = None
    compressed = False
    raw_size = None
    for field, wire, value in _fields(data):
        if field == 1:  # raw
            if wire != 2:
                raise ValueError("invalid raw Blob encoding")
            if payload is not None:
                raise ValueError("multiple Blob payloads")
            payload = value
        elif field == 2:  # raw_size, int32
            if wire != 0:
                raise ValueError("invalid raw_size encoding")
            raw_size = _signed32(value)
        elif field == 3:  # zlib_data
            if wire != 2:
                raise ValueError("invalid zlib Blob encoding")
            if payload is not None:
                raise ValueError("multiple Blob payloads")
            payload = value
            compressed = True
        elif field in (4, 5, 6, 7):  # lzma, bzip2, lz4, zstd
            raise ValueError("unsupported Blob compression")
    if payload is None:
        raise ValueError("Blob has no payload")
    if raw_size is not None and raw_size < 0:
        raise ValueError("negative Blob raw_size")
    decoded = _inflate(payload) if compressed else payload
    if len(decoded) > _MAX_BLOB:
        raise ValueError("Blob exceeds 32 MiB")
    if raw_size is not None and raw_size != len(decoded):
        raise ValueError("Blob raw_size does not match its payload")
    return decoded


# ---------------------------------------------------------------------------
# Disk-backed Way log


class _WayWriter:
    """Append-only log of retained Ways, plus duplicate-id detection."""

    __slots__ = ("_file", "_conn")

    def __init__(self, fileobj, conn):
        self._file = fileobj
        self._conn = conn

    def write(self, way_id, tags, refs):
        try:
            self._conn.execute("INSERT INTO way_ids(id) VALUES(?)", (way_id,))
        except sqlite3.IntegrityError:
            raise ValueError("duplicate way id %d" % way_id) from None
        record = {"id": way_id, "tags": tags, "refs": refs}
        line = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
        self._file.write(line.encode("utf-8") + b"\n")


def _sql_value(value):
    """SQLite integers are int64; keep wider coordinates as exact text."""
    if _I64_MIN <= value <= _I64_MAX:
        return value
    return str(value)


def _iter_way_records(fileobj):
    fileobj.seek(0)
    for line in fileobj:
        yield json.loads(line)


def _missing_node(conn, refs):
    for start in range(0, len(refs), _LOOKUP_BATCH):
        batch = refs[start:start + _LOOKUP_BATCH]
        placeholders = ",".join("?" * len(batch))
        found = {
            row[0]
            for row in conn.execute(
                "SELECT id FROM nodes WHERE id IN (%s)" % placeholders, batch
            )
        }
        for ref in batch:
            if ref not in found:
                return ref
    return None


def _resolve_coords(conn, refs):
    found = {}
    for start in range(0, len(refs), _LOOKUP_BATCH):
        batch = refs[start:start + _LOOKUP_BATCH]
        placeholders = ",".join("?" * len(batch))
        for node_id, lon, lat in conn.execute(
            "SELECT id, lon, lat FROM nodes WHERE id IN (%s)" % placeholders, batch
        ):
            found[node_id] = (lon, lat)
    coords = []
    for ref in refs:
        pair = found.get(ref)
        if pair is None:
            raise ValueError("Way references missing node id %d" % ref)
        coords.append([int(pair[0]), int(pair[1])])
    return coords


# ---------------------------------------------------------------------------
# Public API


def roads(chunks, workdir):
    """Return a closeable iterator of retained OSM ways as polylines.

    Refer to the module docstring for the full contract.
    """
    return _roads(chunks, os.fspath(workdir))


def _roads(chunks, workdir):
    db_path = None
    ways_path = None
    ways_file = None
    conn = None
    try:
        db_fd, db_path = tempfile.mkstemp(
            prefix=".osm-roads-", suffix=".sqlite3", dir=workdir
        )
        os.close(db_fd)
        ways_fd, ways_path = tempfile.mkstemp(
            prefix=".osm-roads-", suffix=".ways", dir=workdir
        )
        ways_file = os.fdopen(ways_fd, "w+b")

        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA journal_mode=OFF")
        conn.execute("PRAGMA synchronous=OFF")
        conn.execute("PRAGMA temp_store=FILE")
        conn.execute("PRAGMA cache_size=-4096")
        conn.execute(
            "CREATE TABLE nodes (id INTEGER PRIMARY KEY, lon, lat) WITHOUT ROWID"
        )
        conn.execute("CREATE TABLE way_ids (id INTEGER PRIMARY KEY) WITHOUT ROWID")

        sink = _NodeSink(conn)
        writer = _WayWriter(ways_file, conn)
        source = _ByteStream(chunks)
        header_seen = False
        data_seen = False

        while not source.at_eof():
            header_length = struct.unpack(">I", source.read(4))[0]
            if header_length > _MAX_BLOB_HEADER:
                raise ValueError("BlobHeader exceeds 64 KiB")
            blob_type, datasize = _parse_blob_header(
                source.read(header_length)
            )
            if datasize > _MAX_SERIALISED_BLOB:
                raise ValueError("Blob exceeds 32 MiB")
            blob = _parse_blob(source.read(datasize))

            if blob_type == "OSMHeader":
                if header_seen:
                    raise ValueError("multiple OSMHeader Blobs")
                if data_seen:
                    raise ValueError("OSMHeader after OSMData")
                _parse_header_block(blob)
                header_seen = True
            else:
                if not header_seen:
                    raise ValueError("OSMData before OSMHeader")
                data_seen = True
                _parse_primitive_block(blob, sink, writer)

        if not header_seen:
            raise ValueError("missing OSMHeader")

        sink.flush()
        conn.commit()
        ways_file.flush()

        # Phase 1: validate every reference of every retained Way.  Nodes may
        # occupy Blobs far after the Way that names them, so this can only be
        # done once the whole stream has been consumed.
        for record in _iter_way_records(ways_file):
            missing = _missing_node(conn, record["refs"])
            if missing is not None:
                raise ValueError("Way references missing node id %d" % missing)

        # Phase 2: resolve coordinates and emit, in source Way order.
        for record in _iter_way_records(ways_file):
            refs = record["refs"]
            yield {
                "id": record["id"],
                "tags": record["tags"],
                "refs": refs,
                "coords": _resolve_coords(conn, refs),
            }
    finally:
        if ways_file is not None:
            try:
                ways_file.close()
            except Exception:
                pass
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
        for path in (db_path, ways_path):
            if path is None:
                continue
            for suffix in ("", "-journal", "-wal", "-shm"):
                try:
                    os.unlink(path + suffix)
                except FileNotFoundError:
                    pass
                except OSError:
                    pass
