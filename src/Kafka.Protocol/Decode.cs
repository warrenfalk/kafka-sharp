using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kafka.Protocol
{
    public static class Decode
    {
        public static MetadataResponse MetadataResponse(int version, ProtocolStreamReader reader) => MetadataResponseImpl.Decode[version](reader);
        public static ApiVersionsResponse ApiVersionsResponse(int version, ProtocolStreamReader reader) => ApiVersionsResponseImpl.Decode[version](reader);

        public static bool Boolean(ProtocolStreamReader reader) => reader.Read(1, Boolean);
        public static bool Boolean(byte[] memory, int offset)
        {
            return memory[offset] != 0;
        }

        public static sbyte Int8(ProtocolStreamReader reader) => reader.Read(1, Int8);
        public static sbyte Int8(byte[] memory, int offset)
        {
            return (sbyte)memory[offset];
        }

        public static short Int16(ProtocolStreamReader reader) => reader.Read(2, Int16);
        public static short Int16(byte[] memory, int offset)
        {
            return (short)
                (memory[offset] << 0x8
                | memory[offset + 1]
                );
        }

        public static int Int32(ProtocolStreamReader reader) => reader.Read(4, Int32);
        public static int Int32(byte[] memory, int offset)
        {
            return (int)
                (memory[offset] << 0x18
                | memory[offset + 1] << 0x10
                | memory[offset + 2] << 0x8
                | memory[offset + 3]
                );
        }

        public static long Int64(ProtocolStreamReader reader) => reader.Read(8, Int64);
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

        public static string String(ProtocolStreamReader reader) => reader.Read(reader.ReadInt16(), StringChars);
        public static string StringChars(byte[] memory, int offset, int length)
        {
            return Encoding.UTF8.GetString(memory, (int)offset, (int)length);
        }

        public static string NullableString(ProtocolStreamReader reader)
        {
            int length = reader.ReadInt16();
            return length < 0 ? null : reader.Read(length, StringChars);
        }

        public static IEnumerable<T> List<T>(ProtocolStreamReader reader, Func<ProtocolStreamReader, T> decodeFunc)
        {
            var count = reader.ReadInt32();
            T[] items = new T[count];
            for (var i = 0; i < count; i++)
                items[i] = decodeFunc(reader);
            return items;
        }
    }
}
