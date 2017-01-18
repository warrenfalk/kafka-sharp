using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kafka.Protocol
{
    public static class Encode
    {
        public static ProtocolWriter Boolean(bool value, ProtocolWriter writer) => writer.Write(value, 1, Boolean);
        public static void Boolean(bool value, byte[] memory, int offset)
        {
            memory[offset] = value ? (byte)1 : (byte)0;
        }

        public static ProtocolWriter Int8(sbyte value, ProtocolWriter writer) => writer.Write(value, 1, Int8);
        public static void Int8(sbyte value, byte[] memory, int offset)
        {
            memory[offset] = (byte)value;
        }

        public static ProtocolWriter Int16(short value, ProtocolWriter writer) => writer.Write(value, 2, Int16);
        public static void Int16(short value, byte[] memory, int offset)
        {
            memory[0] = (byte)(value >> 0x8 & 0xFF);
            memory[1] = (byte)(value & 0xFF);
        }

        public static ProtocolWriter Int32(int value, ProtocolWriter writer) => writer.Write(value, 4, Int32);
        public static void Int32(int value, byte[] memory, int offset)
        {
            memory[0] = (byte)(value >> 0x18 & 0xFF);
            memory[1] = (byte)(value >> 0x10 & 0xFF);
            memory[2] = (byte)(value >> 0x8 & 0xFF);
            memory[3] = (byte)(value & 0xFF);
        }


        public static ProtocolWriter Int64(long value, ProtocolWriter writer) => writer.Write(value, 8, Int64);
        public static void Int64(long value, byte[] memory, int offset)
        {
            memory[0] = (byte)(value >> 0x38 & 0xFF);
            memory[1] = (byte)(value >> 0x30 & 0xFF);
            memory[2] = (byte)(value >> 0x28 & 0xFF);
            memory[3] = (byte)(value >> 0x20 & 0xFF);
            memory[4] = (byte)(value >> 0x18 & 0xFF);
            memory[5] = (byte)(value >> 0x10 & 0xFF);
            memory[6] = (byte)(value >> 0x8 & 0xFF);
            memory[7] = (byte)(value & 0xFF);
        }

        public static ProtocolWriter String(string value, ProtocolWriter writer) => value == null ? writer.WriteInt16(-1) : writer.Write(value, 2 + Encoding.UTF8.GetByteCount(value), String);
        public static void String(string value, byte[] memory, int offset, int size)
        {
            Int16((short)(size - 2), memory, offset);
            StringChars(value, memory, offset + 2);
        }
        public static void StringChars(string value, byte[] memory, int offset)
        {
            Encoding.UTF8.GetBytes(value, 0, value.Length, memory, offset);
        }

        public static ProtocolWriter List<T>(IEnumerable<T> value, ProtocolWriter writer, Func<T, ProtocolWriter, ProtocolWriter> encodeFunc)
        {
            var count = value.Count();
            writer.WriteInt32(count);
            foreach (var t in value)
                encodeFunc(t, writer);
            return writer;
        }

    }
}
