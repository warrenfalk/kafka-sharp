using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Kafka.Protocol
{
    public class ProtocolReader
    {
        Slice Data { get; }
        int Offset { get; set; }

        public ProtocolReader(Slice data)
        {
            Data = data;
        }

        public Slice ReadRaw(int size)
        {
            size = Math.Min(Data.Length - Offset, size);
            var slice = Data.Subslice(Offset, size);
            Offset += size;
            return slice;
        }

        public bool ReadBoolean() => Read(1, Decode.Boolean);

        public sbyte ReadInt8() => Read(1, Decode.Int8);

        public short ReadInt16() => Read(2, Decode.Int16);

        public int ReadInt32() => Read(4, Decode.Int32);

        public long ReadInt64() => Read(8, Decode.Int64);

        public string ReadString() => Read(ReadInt16(), Decode.StringChars);

        public string ReadNullableString()
        {
            var size = ReadInt16();
            if (size == -1)
                return null;
            return Read(size, Decode.StringChars);
        }

        public IEnumerable<T> ReadList<T>(Func<ProtocolReader, T> decodeFunc)
        {
            var count = ReadInt32();
            T[] items = new T[count];
            for (var i = 0; i < count; i++)
                items[i] = decodeFunc(this);
            return items;
        }

        public T Read<T>(int size, Func<ProtocolReader, T> decodeFunc)
        {
            var sub = new ProtocolReader(Data.Subslice(Offset, size));
            Offset += size;
            return decodeFunc(sub);
        }

        public T Read<T>(int size, Func<Slice, T> decode)
        {
            T value = decode(Data.Subslice(Offset, size));
            Offset += size;
            return value;
        }
    }
}
