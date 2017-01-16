using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Kafka.Protocol
{
    public class ProtocolStreamReader
    {
        Readable Stream { get; }
        byte[] Buffer { get; }

        public ProtocolStreamReader(Readable stream)
        {
            Stream = stream;
            Buffer = new byte[512];
        }

        public ProtocolStreamReader(Stream stream)
            : this(stream.AsReadable())
        { }

        public IEnumerable<T> ReadList<T>(Func<ProtocolStreamReader, T> decodeFunc) => Decode.List<T>(this, decodeFunc);

        public bool ReadBoolean() => Decode.Boolean(this);

        public sbyte ReadInt8() => Decode.Int8(this);

        public short ReadInt16() => Decode.Int16(this);

        public int ReadInt32() => Decode.Int32(this);

        public long ReadInt64() => Decode.Int64(this);

        public string ReadString() => Decode.String(this);

        public string ReadNullableString() => Decode.NullableString(this);

        public T Read<T>(int size, Func<byte[], int, T> decode)
        {
            var buffer = WaitFor(Stream, size, Buffer);
            var value = decode(buffer, 0);
            return value;
        }

        public T Read<T>(int size, Func<byte[], int, int, T> decode)
        {
            var buffer = WaitFor(Stream, size, Buffer);
            var value = decode(buffer, 0, size);
            return value;
        }

        private static byte[] WaitFor(Readable stream, int size, byte[] recycled)
        {
            var buffer = (size > recycled.Length) ? new byte[size] : recycled;
            int cursor = 0;
            while (cursor < size)
            {
                int advance = stream.Read(buffer, cursor, size - cursor);
                if (advance == 0)
                    throw new EndOfStreamException();
                cursor += advance;
            }
            return buffer;
        }
    }
}
