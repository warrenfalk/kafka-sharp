using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kafka.Protocol
{
    /// <summary>
    /// Represents a binary value with convenience methods for conversion to/from strings and byte arrays, etc.
    /// </summary>
    public sealed class BinaryValue
    {
        public Slice Slice { get; }
        public int Length => Slice.Length;
        public byte this[int index] => Slice[index];

        public BinaryValue(Slice slice)
        {
            Slice = slice;
        }

        public BinaryValue(byte[] bytes)
            : this(new Slice(bytes))
        {
        }

        public BinaryValue(string stringValue)
            : this(Encoding.UTF8.GetBytes(stringValue))
        {
        }

        public override string ToString() => ToString(Encoding.UTF8);

        public string ToString(Encoding encoding) => encoding.GetString(Slice.Buffer, Slice.Offset, Slice.Length);

        public Stream GetStream() => Slice.GetStream();

        public static implicit operator BinaryValue(string value) => new BinaryValue(value);

        public static implicit operator BinaryValue(byte[] value) => new BinaryValue(value);

        public static implicit operator BinaryValue(Slice value) => new BinaryValue(value);

        public static implicit operator BinaryValue(ArraySegment<byte> value) => new BinaryValue(new Slice(value));

        public static implicit operator string(BinaryValue value) => value?.ToString();

        public static implicit operator byte[](BinaryValue value) => value?.Slice.ToArray();

        public static implicit operator Slice(BinaryValue value) => value.Slice;

        public static implicit operator ArraySegment<byte>(BinaryValue value) => value.Slice;
    }
}
