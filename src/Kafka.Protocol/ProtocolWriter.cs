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

        public ProtocolWriter WriteRaw(Slice slice) => WriteRaw(slice.Buffer, slice.Offset, slice.Length);

        public ProtocolWriter WriteRaw(byte[] buffer, int offset, int length)
        {
            Stream.Write(buffer, offset, length);
            return this;
        }

        public ProtocolWriter WriteRaw(byte[] buffer) => WriteRaw(buffer, 0, buffer.Length);

        public ProtocolWriter WriteList<T>(IEnumerable<T> value, Func<T, ProtocolWriter, ProtocolWriter> encodeFunc) => Encode.List<T>(value, this, encodeFunc);

        public ProtocolWriter WriteBoolean(bool value) => Encode.Boolean(value, this);

        public ProtocolWriter WriteInt8(sbyte value) => Encode.Int8(value, this);

        public ProtocolWriter WriteInt16(short value) => Encode.Int16(value, this);

        public ProtocolWriter WriteInt32(int value) => Encode.Int32(value, this);

        public ProtocolWriter WriteInt64(long value) => Encode.Int64(value, this);

        public ProtocolWriter WriteString(string value) => Encode.String(value, this);


        public ProtocolWriter WriteNullableString(string value) => Encode.String(value, this);

        public ProtocolWriter WriteBytes(BinaryValue value) => Encode.Bytes(value, this);

        public ProtocolWriter WriteMessageSet(MessageSet messageSet) => MessageSet.Encode(messageSet, this);

        public ProtocolWriter Write<T>(T value, int size, Action<T, byte[], int> encode)
        {
            var buffer = (size <= Buffer.Length) ? Buffer : new byte[size];
            encode(value, buffer, 0);
            Stream.Write(buffer, 0, size);
            return this;
        }

        public ProtocolWriter Write<T>(T value, int size, Action<T, byte[], int, int> encode)
        {
            var buffer = (size <= Buffer.Length) ? Buffer : new byte[size];
            encode(value, buffer, 0, size);
            Stream.Write(buffer, 0, size);
            return this;
        }
    }
}
