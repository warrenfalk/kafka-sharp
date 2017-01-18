using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Kafka.Protocol
{
    /// <summary>
    /// The purpose of this structure is to be able to pass around raw data without copying it to multiple smaller buffers
    /// This is not technically an immutable structure, because direct access to the buffer allows slices to change data outside their own range
    /// Prevention of that requires another immutable class on top of slice
    /// But such a class really has to be custom-written because it will be the only thing that can read from the buffer and so it must have knowledge of the protocol
    /// A crude immutable can be written which can simply copy segments out to other buffers, but this is an extra copy step in most cases
    /// </summary>
    public struct Slice
    {
        public byte[] Buffer { get; }
        public int Offset { get; }
        public int Length { get; }

        public static Slice Empty { get; } = new Slice(0);

        public Slice(int length)
            : this(new byte[length], 0, length)
        {
        }

        public Slice(Slice source, int offset, int length)
            : this(source.Buffer, source.Offset + offset, length)
        {
        }

        public Slice(byte[] buffer)
            : this(buffer, 0, buffer.Length)
        { }

        public Slice(byte[] buffer, int offset, int length)
        {
            if (offset < 0 || (offset + length) > buffer.Length)
                throw new IndexOutOfRangeException();
            Buffer = buffer;
            Offset = offset;
            Length = length;
        }

        public Slice(ArraySegment<byte> value)
            : this(value.Array, value.Offset, value.Count)
        {
        }

        public byte this[int index] => Buffer[CheckBounds(index + Offset)];

        int CheckBounds(int index)
        {
            if (index < Offset || index >= (Offset + Length))
                throw new IndexOutOfRangeException();
            return index;
        }

        public Slice At(int offset)
        {
            return Subslice(offset, Length - offset);
        }

        public Slice Subslice(int offset, int length)
        {
            return new Slice(this, offset, length);
        }

        public override string ToString()
        {
            return string.Join("", Buffer.Skip((int)Offset).Take(Math.Min(100, (int)Length)).Select(b => b.ToString("x2"))) + (Length > 100 ? "..." : "");
        }

        public byte[] ToArray()
        {
            var array = new byte[Length];
            if (Offset > int.MaxValue || Length > int.MaxValue)
                throw new NotImplementedException("TODO: implement 64 bit ToArray()");
            System.Buffer.BlockCopy(Buffer, (int)Offset, array, 0, (int)Length);
            return array;
        }

        public Stream GetStream() => new SliceStream(this);

        public static implicit operator ArraySegment<byte>(Slice slice) => new ArraySegment<byte>(slice.Buffer, slice.Offset, slice.Length);

        public static implicit operator Slice(ArraySegment<byte> segment) => new Slice(segment);

        private class SliceStream : Stream
        {
            public Slice Slice { get; }
            public int Offset { get; set; }

            public override bool CanRead => true;
            public override bool CanSeek => true;
            public override bool CanWrite => false;
            public override long Length => Slice.Length;
            public override long Position
            {
                get { return Offset; }
                set { Seek(value, SeekOrigin.Begin); }
            }

            public SliceStream(Slice slice) { Slice = slice; }

            public override void Flush() { throw new NotImplementedException(); }
            public override void SetLength(long value) { throw new NotImplementedException(); }
            public override void Write(byte[] buffer, int offset, int count) { throw new NotImplementedException(); }

            public override int Read(byte[] buffer, int offset, int count)
            {
                count = Math.Min(Slice.Length - Offset, count);
                System.Buffer.BlockCopy(Slice.Buffer, Slice.Offset + Offset, buffer, offset, count);
                Offset += count;
                return count;
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                switch (origin)
                {
                    case SeekOrigin.Begin:
                        if (offset < 0 || offset > Slice.Length)
                            throw new InvalidOperationException("Seek beyond bounds");
                        return Offset = (int)offset;
                    case SeekOrigin.Current:
                        return Seek(Offset + offset, SeekOrigin.Begin);
                    case SeekOrigin.End:
                        return Seek(Slice.Length + offset, SeekOrigin.Begin);
                    default:
                        throw new InvalidOperationException("Unknown seek origin");
                }
            }

        }
    }

}
