using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Kafka.Protocol
{
    public class RequestWriter
    {
        Writable Stream { get; }
        RequestBuffer Buffer { get; }

        public RequestWriter(Writable stream)
        {
            Stream = stream;
            Buffer = new RequestBuffer();
        }

        public void WriteRequest(Request request, int correlationId, string clientId)
        {
            Buffer.Clear();

            var innerWriter = new ProtocolStreamWriter(Buffer);
            innerWriter.WriteInt16((short)request.ApiKey);
            innerWriter.WriteInt16(request.ApiVersion);
            innerWriter.WriteInt32(correlationId);
            innerWriter.WriteNullableString(clientId);
            request.WriteTo(innerWriter);

            Buffer.WriteRequestTo(Stream);
            Buffer.Clear();
        }

        class RequestBuffer : Writable
        {
            LinkedList<byte[]> Buffers { get; }
            int Cursor { get; set; }
            int SegmentCursor { get; set; }

            const int SegmentSize = 8192;

            public RequestBuffer()
            {
                Buffers = new LinkedList<byte[]>();
                Buffers.AddFirst(new byte[SegmentSize]);
            }

            public void Clear()
            {
                Cursor = 4;
                SegmentCursor = 4;
                // save the first buffer
                // clear the rest
                var first = Buffers.First.Value;
                Buffers.Clear();
                Buffers.AddFirst(first);
            }

            public void WriteRequestTo(Writable stream)
            {
                // Write the size at the beginning
                Encode.Int32(Cursor - 4, Buffers.First.Value, 0);
                var remain = Cursor;
                foreach (var buffer in Buffers)
                {
                    var length = Math.Min(remain, buffer.Length);
                    stream.Write(buffer, 0, length);
                    remain -= length;
                }
            }

            public void Write(byte[] buffer, int offset, int length)
            {
                var remain = length;
                while (remain > 0)
                {
                    var segment = Buffers.Last.Value;
                    var available = segment.Length - SegmentCursor;
                    if (available == 0)
                    {
                        var newSegment = new byte[SegmentSize];
                        SegmentCursor = 0;
                        Buffers.AddLast(newSegment);
                        segment = newSegment;
                        available = newSegment.Length;
                    }
                    var copyLength = Math.Min(available, length);
                    System.Buffer.BlockCopy(buffer, offset, segment, SegmentCursor, copyLength);
                    SegmentCursor += copyLength;
                    Cursor += copyLength;
                    remain -= copyLength;
                }
            }
        }
    }
}
