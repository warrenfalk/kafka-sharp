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
        int BufferCursor { get; set; }

        public ProtocolStreamReader(Readable stream)
        {
            Stream = stream;
            Buffer = new byte[512];
        }

        public ProtocolStreamReader(Stream stream)
            : this(stream.AsReadable())
        { }

        public IEnumerable<T> ReadList<T>(Func<ProtocolStreamReader, T> parseFunc) => Parse.List<T>(this, parseFunc);

        public bool ReadBoolean() => Parse.Boolean(this);

        public sbyte ReadInt8() => Parse.Int8(this);

        public short ReadInt16() => Parse.Int16(this);

        public int ReadInt32() => Parse.Int32(this);

        public long ReadInt64() => Parse.Int64(this);

        public string ReadString() => Parse.String(this);

        public string ReadNullableString() => Parse.NullableString(this);

        public T Read<T>(int size, Func<byte[], long, T> parse)
        {
            WaitFor(size);
            var value = parse(Buffer, 0);
            Reset();
            return value;
        }

        public T Read<T>(int size, Func<byte[], long, long, T> parse)
        {
            WaitFor(size);
            var value = parse(Buffer, 0, size);
            Reset();
            return value;
        }

        private void WaitFor(int size)
        {
            while (BufferCursor < size)
            {
                int advance = Stream.Read(Buffer, BufferCursor, size - BufferCursor);
                if (advance == 0)
                    throw new EndOfStreamException();
                BufferCursor += advance;
            }
        }

        private void Reset()
        {
            BufferCursor = 0;
        }
    }
}
