using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Kafka.Protocol
{
    public class ProtocolWriter
    {
        Writable Stream { get; }
        byte[] Buffer { get; }

        public ProtocolWriter(Writable stream)
        {
            Stream = stream;
            Buffer = new byte[512];
        }

        public ProtocolWriter(Stream stream)
            : this(stream.AsWritable())
        { }

        public void WriteRaw(Slice slice) => Stream.Write(slice.Buffer, slice.Offset, slice.Length);

        public void WriteRaw(byte[] buffer, int offset, int length) => Stream.Write(buffer, offset, length);

        public void WriteRaw(byte[] buffer) => Stream.Write(buffer, 0, buffer.Length);

        public void WriteList<T>(IEnumerable<T> value, Action<T, ProtocolWriter> encodeFunc) => Encode.List<T>(value, this, encodeFunc);

        public void WriteBoolean(bool value) => Encode.Boolean(value, this);

        public void WriteInt8(sbyte value) => Encode.Int8(value, this);

        public void WriteInt16(short value) => Encode.Int16(value, this);

        public void WriteInt32(int value) => Encode.Int32(value, this);

        public void WriteInt64(long value) => Encode.Int64(value, this);

        public void WriteString(string value) => Encode.String(value, this);

        public void WriteNullableString(string value) => Encode.NullableString(value, this);

        public void WriteMessageSet(MessageSet messageSet) => MessageSet.Encode(messageSet, this);

        public void Write<T>(T value, int size, Action<T, byte[], int> encode)
        {
            var buffer = (size <= Buffer.Length) ? Buffer : new byte[size];
            encode(value, buffer, 0);
            Stream.Write(buffer, 0, size);
        }

        public void Write<T>(T value, int size, Action<T, byte[], int, int> encode)
        {
            var buffer = (size <= Buffer.Length) ? Buffer : new byte[size];
            encode(value, buffer, 0, size);
            Stream.Write(buffer, 0, size);
        }
    }
}
