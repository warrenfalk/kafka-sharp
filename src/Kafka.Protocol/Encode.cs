using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kafka.Protocol
{
    public static class Encode
    {
        public static void Boolean(bool value, ProtocolStreamWriter writer) => writer.Write(value, 1, Boolean);
        public static void Boolean(bool value, byte[] memory, int offset)
        {
            memory[offset] = value ? (byte)1 : (byte)0;
        }

        public static void Int8(sbyte value, ProtocolStreamWriter writer) => writer.Write(value, 1, Int8);
        public static void Int8(sbyte value, byte[] memory, int offset)
        {
            memory[offset] = (byte)value;
        }

        public static void Int16(short value, ProtocolStreamWriter writer) => writer.Write(value, 2, Int16);
        public static void Int16(short value, byte[] memory, int offset)
        {
            memory[0] = (byte)(value >> 0x8 & 0xFF);
            memory[1] = (byte)(value & 0xFF);
        }

        public static void Int32(int value, ProtocolStreamWriter writer) => writer.Write(value, 4, Int32);
        public static void Int32(int value, byte[] memory, int offset)
        {
            memory[0] = (byte)(value >> 0x18 & 0xFF);
            memory[1] = (byte)(value >> 0x10 & 0xFF);
            memory[2] = (byte)(value >> 0x8 & 0xFF);
            memory[3] = (byte)(value & 0xFF);
        }


        public static void Int64(long value, ProtocolStreamWriter writer) => writer.Write(value, 8, Int64);
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

        public static void String(string value, ProtocolStreamWriter writer) => writer.Write(value, 2 + Encoding.UTF8.GetByteCount(value), String);
        public static void String(string value, byte[] memory, int offset, int size)
        {
            Int16((short)(size - 2), memory, offset);
            StringChars(value, memory, offset + 2);
        }
        public static void StringChars(string value, byte[] memory, int offset)
        {
            Encoding.UTF8.GetBytes(value, 0, value.Length, memory, offset);
        }

        public static void NullableString(string value, ProtocolStreamWriter writer)
        {
            if (value == null)
                writer.WriteInt16(-1);
            else
                String(value, writer);
        }

        public static void List<T>(IEnumerable<T> value, ProtocolStreamWriter writer, Action<T, ProtocolStreamWriter> encodeFunc)
        {
            var count = value.Count();
            writer.WriteInt32(count);
            foreach (var t in value)
                encodeFunc(t, writer);
        }

    }
}
