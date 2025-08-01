import struct
from typing import Literal
from unittest import mock

import pytest

import aiokafka.codec
from aiokafka.errors import CorruptRecordException, UnsupportedCodecError
from aiokafka.record.legacy_records import LegacyRecordBatch, LegacyRecordBatchBuilder


@pytest.mark.parametrize("magic", [0, 1])
@pytest.mark.parametrize(
    "key,value,checksum",
    [
        (b"test", b"Super", [278251978, -2095076219]),
        (b"test", None, [580701536, 164492157]),
        (None, b"Super", [2797021502, 3315209433]),
        (b"", b"Super", [1446809667, 890351012]),
        (b"test", b"", [4230475139, 3614888862]),
    ],
)
def test_read_write_serde_v0_v1_no_compression(
    magic: Literal[0, 1],
    key: bytes | None,
    value: bytes | None,
    checksum: tuple[int, int],
) -> None:
    builder = LegacyRecordBatchBuilder(
        magic=magic, compression_type=0, batch_size=1024 * 1024
    )
    builder.append(0, timestamp=9999999, key=key, value=value)
    buffer = builder.build()

    batch = LegacyRecordBatch(buffer, magic)
    assert batch.validate_crc()

    assert batch.is_control_batch is False
    assert batch.is_transactional is False
    assert batch.producer_id is None
    assert batch.next_offset == 1

    msgs = list(batch)
    assert len(msgs) == 1
    msg = msgs[0]

    assert msg.offset == 0
    assert msg.timestamp == (9999999 if magic else None)
    assert msg.timestamp_type == (0 if magic else None)
    assert msg.key == key
    assert msg.value == value
    assert msg.checksum == checksum[magic] & 0xFFFFFFFF


@pytest.mark.parametrize(
    "compression_type, magic",
    [
        (LegacyRecordBatch.CODEC_GZIP, 0),
        (LegacyRecordBatch.CODEC_SNAPPY, 0),
        # We don't support LZ4 for kafka 0.8/0.9
        (LegacyRecordBatch.CODEC_GZIP, 1),
        (LegacyRecordBatch.CODEC_SNAPPY, 1),
        (LegacyRecordBatch.CODEC_LZ4, 1),
    ],
)
def test_read_write_serde_v0_v1_with_compression(
    compression_type: Literal[0x01, 0x02, 0x03], magic: Literal[0, 1]
) -> None:
    builder = LegacyRecordBatchBuilder(
        magic=magic, compression_type=compression_type, batch_size=1024 * 1024
    )
    for offset in range(10):
        builder.append(offset, timestamp=9999999, key=b"test", value=b"Super")
    buffer = builder.build()

    # Broker will set the offset to a proper last offset value
    struct.pack_into(">q", buffer, 0, 9)

    batch = LegacyRecordBatch(buffer, magic)
    assert batch.validate_crc()

    assert batch.is_control_batch is False
    assert batch.is_transactional is False
    assert batch.producer_id is None
    assert batch.next_offset == 10

    msgs = list(batch)

    for offset, msg in enumerate(msgs):
        assert msg.offset == offset
        assert msg.timestamp == (9999999 if magic else None)
        assert msg.timestamp_type == (0 if magic else None)
        assert msg.key == b"test"
        assert msg.value == b"Super"
        assert msg.checksum == (-2095076219 if magic else 278251978) & 0xFFFFFFFF


@pytest.mark.parametrize("magic", [0, 1])
def test_written_bytes_equals_size_in_bytes(magic: Literal[0, 1]) -> None:
    key = b"test"
    value = b"Super"
    builder = LegacyRecordBatchBuilder(
        magic=magic, compression_type=0, batch_size=1024 * 1024
    )

    size_in_bytes = builder.size_in_bytes(0, timestamp=9999999, key=key, value=value)

    pos = builder.size()
    builder.append(0, timestamp=9999999, key=key, value=value)

    assert builder.size() - pos == size_in_bytes


@pytest.mark.parametrize("magic", [0, 1])
def test_legacy_batch_builder_validates_arguments(magic: Literal[0, 1]) -> None:
    builder = LegacyRecordBatchBuilder(
        magic=magic, compression_type=0, batch_size=1024 * 1024
    )

    # Key should not be str
    with pytest.raises(TypeError):
        builder.append(
            0,
            timestamp=9999999,
            key="some string",  # type: ignore[arg-type]
            value=None,
        )

    # Value should not be str
    with pytest.raises(TypeError):
        builder.append(
            0,
            timestamp=9999999,
            key=None,
            value="some string",  # type: ignore[arg-type]
        )

    # Timestamp should be of proper type (timestamp is ignored for magic == 0)
    if magic != 0:
        with pytest.raises(TypeError):
            builder.append(
                0,
                timestamp="1243812793",  # type: ignore[arg-type]
                key=None,
                value=b"some string",
            )

    # Offset of invalid type
    with pytest.raises(TypeError):
        builder.append(
            "0",  # type: ignore[arg-type]
            timestamp=9999999,
            key=None,
            value=b"some string",
        )

    # Unknown struct errors are passed through. These are theoretical and
    # indicate a bug in the implementation. The C implementation locates
    # _encode_msg elsewhere and is less vulnerable to such bugs since it's
    # statically typed, so we skip the test there.
    if hasattr(builder, "_encode_msg"):
        with mock.patch.object(builder, "_encode_msg") as mocked:
            err = struct.error("test error")
            mocked.side_effect = err
            with pytest.raises(struct.error) as excinfo:
                builder.append(0, timestamp=None, key=None, value=b"some string")
            assert excinfo.value == err

    # Ok to pass value as None
    builder.append(0, timestamp=9999999, key=b"123", value=None)

    # Timestamp can be None
    builder.append(1, timestamp=None, key=None, value=b"some string")

    # Ok to pass offsets in not incremental order. This should not happen thou
    builder.append(5, timestamp=9999999, key=b"123", value=None)

    # in case error handling code fails to fix inner buffer in builder
    assert len(builder.build()) == 119 if magic else 95


@pytest.mark.parametrize("magic", [0, 1])
def test_legacy_correct_metadata_response(magic: Literal[0, 1]) -> None:
    builder = LegacyRecordBatchBuilder(
        magic=magic, compression_type=0, batch_size=1024 * 1024
    )
    meta = builder.append(0, timestamp=9999999, key=b"test", value=b"Super")

    assert meta is not None
    assert meta.offset == 0
    assert meta.timestamp == (9999999 if magic else -1)
    assert meta.crc == (-2095076219 if magic else 278251978) & 0xFFFFFFFF
    assert repr(meta) == (
        f"LegacyRecordMetadata(offset=0, crc={meta.crc}, size={meta.size}, "
        f"timestamp={meta.timestamp})"
    )


@pytest.mark.parametrize("magic", [0, 1])
def test_legacy_batch_size_limit(magic: Literal[0, 1]) -> None:
    # First message can be added even if it's too big
    builder = LegacyRecordBatchBuilder(magic=magic, compression_type=0, batch_size=1024)
    meta = builder.append(0, timestamp=None, key=None, value=b"M" * 2000)
    assert meta is not None
    assert meta.size > 0
    assert meta.crc is not None
    assert meta.offset == 0
    assert meta.timestamp is not None
    assert len(builder.build()) > 2000

    builder = LegacyRecordBatchBuilder(magic=magic, compression_type=0, batch_size=1024)
    meta = builder.append(0, timestamp=None, key=None, value=b"M" * 700)
    assert meta is not None
    meta = builder.append(1, timestamp=None, key=None, value=b"M" * 700)
    assert meta is None
    meta = builder.append(2, timestamp=None, key=None, value=b"M" * 700)
    assert meta is None
    assert len(builder.build()) < 1000


@pytest.mark.parametrize(
    "compression_type,name,checker_name",
    [
        (LegacyRecordBatch.CODEC_GZIP, "gzip", "has_gzip"),
        (LegacyRecordBatch.CODEC_SNAPPY, "snappy", "has_snappy"),
    ],
)
def test_unavailable_codec(
    compression_type: Literal[0x01, 0x02], name: str, checker_name: str
) -> None:
    builder = LegacyRecordBatchBuilder(
        magic=0, compression_type=compression_type, batch_size=1024
    )
    builder.append(0, timestamp=None, key=None, value=b"M")
    correct_buffer = builder.build()

    with mock.patch.object(aiokafka.codec, checker_name) as mocked:
        mocked.return_value = False
        # Check that builder raises error
        builder = LegacyRecordBatchBuilder(
            magic=0, compression_type=compression_type, batch_size=1024
        )
        error_msg = f"Libraries for {name} compression codec not found"
        with pytest.raises(UnsupportedCodecError, match=error_msg):
            builder.append(0, timestamp=None, key=None, value=b"M")
            builder.build()

        # Check that reader raises same error
        batch = LegacyRecordBatch(bytes(correct_buffer), 0)
        with pytest.raises(UnsupportedCodecError, match=error_msg):
            list(batch)


def test_unsupported_yet_codec() -> None:
    compression_type = LegacyRecordBatch.CODEC_MASK  # It doesn't exist
    builder = LegacyRecordBatchBuilder(
        magic=0,
        compression_type=compression_type,
        batch_size=1024,
    )
    with pytest.raises(UnsupportedCodecError):
        builder.append(0, timestamp=None, key=None, value=b"M")
        builder.build()


ATTRIBUTES_OFFSET = 17
TIMESTAMP_OFFSET = 18
TIMESTAMP_TYPE_MASK = 0x08


def _make_compressed_batch(magic: Literal[0, 1]) -> bytearray:
    builder = LegacyRecordBatchBuilder(
        magic=magic,
        compression_type=LegacyRecordBatch.CODEC_GZIP,
        batch_size=1024 * 1024,
    )
    for offset in range(10):
        builder.append(offset, timestamp=9999999, key=b"test", value=b"Super")
    return builder.build()


def test_read_log_append_time_v1() -> None:
    buffer = _make_compressed_batch(1)

    # As Builder does not support creating data with `timestamp_type==1` we
    # patch the result manually

    buffer[ATTRIBUTES_OFFSET] |= TIMESTAMP_TYPE_MASK
    expected_timestamp = 10000000
    struct.pack_into(">q", buffer, TIMESTAMP_OFFSET, expected_timestamp)

    batch = LegacyRecordBatch(buffer, 1)
    msgs = list(batch)

    for offset, msg in enumerate(msgs):
        assert msg.offset == offset
        assert msg.timestamp == expected_timestamp
        assert msg.timestamp_type == 1


@pytest.mark.parametrize("magic", [0, 1])
def test_reader_corrupt_record_v0_v1(magic: Literal[0, 1]) -> None:
    buffer = _make_compressed_batch(magic)
    len_offset = 8

    # If the wrapper of compressed messages has a key it will just be ignored.
    key_offset = 26 if magic else 18
    new_buffer = (
        buffer[:key_offset]
        + b"\x00\x00\x00\x03123"  # Insert some KEY into wrapper
        + buffer[key_offset + 4 :]  # Ignore the 4 byte -1 value for old KEY==None
    )
    struct.pack_into(">i", new_buffer, len_offset, len(new_buffer) - 12)
    batch = LegacyRecordBatch(new_buffer, magic)
    msgs = list(batch)
    for offset, msg in enumerate(msgs):
        assert msg.offset == offset
        assert msg.timestamp == (9999999 if magic else None)
        assert msg.timestamp_type == (0 if magic else None)
        assert msg.key == b"test"
        assert msg.value == b"Super"
        assert msg.checksum == (-2095076219 if magic else 278251978) & 0xFFFFFFFF

    # If the wrapper does not contain a `value` it's corrupted
    value_offset = 30 if magic else 22
    new_buffer = (
        buffer[:value_offset]
        + b"\xff\xff\xff\xff"  # Set `value` to None by altering size to -1
    )
    struct.pack_into(">i", new_buffer, len_offset, len(new_buffer) - 12)
    with pytest.raises(
        CorruptRecordException, match="Value of compressed message is None"
    ):
        batch = LegacyRecordBatch(new_buffer, magic)
        list(batch)


def test_record_overhead() -> None:
    known = {
        0: 14,
        1: 22,
    }
    for magic, size in known.items():
        assert LegacyRecordBatchBuilder.record_overhead(magic) == size
