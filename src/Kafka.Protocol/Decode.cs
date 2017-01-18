using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kafka.Protocol
{
    public static class Decode
    {
        public static ProduceResponse ProduceResponse(int version, ProtocolReader reader) => ProduceResponseImpl.Decode[version](reader);
        public static FetchResponse FetchResponse(int version, ProtocolReader reader) => FetchResponseImpl.Decode[version](reader);
        public static OffsetsResponse OffsetsResponse(int version, ProtocolReader reader) => OffsetsResponseImpl.Decode[version](reader);
        public static MetadataResponse MetadataResponse(int version, ProtocolReader reader) => MetadataResponseImpl.Decode[version](reader);
        public static OffsetCommitResponse OffsetCommitResponse(int version, ProtocolReader reader) => OffsetCommitResponseImpl.Decode[version](reader);
        public static OffsetFetchResponse OffsetFetchResponse(int version, ProtocolReader reader) => OffsetFetchResponseImpl.Decode[version](reader);
        public static GroupCoordinatorResponse GroupCoordinatorResponse(int version, ProtocolReader reader) => GroupCoordinatorResponseImpl.Decode[version](reader);
        public static JoinGroupResponse JoinGroupResponse(int version, ProtocolReader reader) => JoinGroupResponseImpl.Decode[version](reader);
        public static SyncGroupResponse SyncGroupResponse(int version, ProtocolReader reader) => SyncGroupResponseImpl.Decode[version](reader);
        public static DescribeGroupsResponse DescribeGroupsResponse(int version, ProtocolReader reader) => DescribeGroupsResponseImpl.Decode[version](reader);
        public static ListGroupsResponse ListGroupsResponse(int version, ProtocolReader reader) => ListGroupsResponseImpl.Versions[version](reader);
        public static SaslHandshakeResponse SaslHandshakeResponse(int version, ProtocolReader reader) => SaslHandshakeResponseImpl.Versions[version](reader);
        public static ApiVersionsResponse ApiVersionsResponse(int version, ProtocolReader reader) => ApiVersionsResponseImpl.Decode[version](reader);

        public static bool Boolean(this Slice slice) => Boolean(slice.Buffer, slice.Offset);
        public static bool Boolean(byte[] memory, int offset)
        {
            return memory[offset] != 0;
        }

        public static sbyte Int8(this Slice slice) => Int8(slice.Buffer, slice.Offset);
        public static sbyte Int8(byte[] memory, int offset)
        {
            return (sbyte)memory[offset];
        }

        public static short Int16(this Slice slice) => Int16(slice.Buffer, slice.Offset);
        public static short Int16(byte[] memory, int offset)
        {
            return (short)
                (memory[offset] << 0x8
                | memory[offset + 1]
                );
        }

        public static int Int32(this Slice slice) => Int32(slice.Buffer, slice.Offset);
        public static int Int32(byte[] memory, int offset)
        {
            return (int)
                (memory[offset] << 0x18
                | memory[offset + 1] << 0x10
                | memory[offset + 2] << 0x8
                | memory[offset + 3]
                );
        }

        public static long Int64(this Slice slice) => Int64(slice.Buffer, slice.Offset);
        public static long Int64(byte[] memory, int offset)
        {
            return (long)
                ((long)memory[offset] << 0x38
                | (long)memory[offset + 1] << 0x30
                | (long)memory[offset + 2] << 0x28
                | (long)memory[offset + 3] << 0x20
                | (long)memory[offset + 4] << 0x18
                | (long)memory[offset + 5] << 0x10
                | (long)memory[offset + 6] << 0x8
                | (long)memory[offset + 7]
                );
        }

        public static string StringChars(this Slice slice) => StringChars(slice.Buffer, slice.Offset, slice.Length);
        public static string StringChars(byte[] memory, int offset, int length)
        {
            return Encoding.UTF8.GetString(memory, offset, length);
        }

        public static BinaryValue Bytes(this Slice slice) => BinaryValue(slice.Buffer, slice.Offset, slice.Length);
        public static BinaryValue BinaryValue(byte[] memory, int offset, int length)
        {
            return new Protocol.BinaryValue(new Slice(memory, offset, length));
        }
    }
}
