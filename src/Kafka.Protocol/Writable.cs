using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Kafka.Protocol
{
    public interface Writable
    {
        void Write(byte[] buffer, int offset, int length);
    }

    public static class WritableExtensions
    {
        public static Writable AsWritable(this Stream stream) => new WritableStream(stream);

        private class WritableStream : Writable
        {
            readonly Stream wrapped;

            public WritableStream(Stream stream)
            {
                wrapped = stream;
            }

            public void Write(byte[] buffer, int offset, int count) => wrapped.Write(buffer, offset, count);
        }
    }
}
