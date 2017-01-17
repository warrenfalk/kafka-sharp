using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Kafka.Protocol
{
    public interface Readable
    {
        int Read(byte[] buffer, int offset, int length);
        bool IsAtEnd();
        Readable SubReadable(int size);
    }

    public static class ReadableExtension
    {
        public static Readable AsReadable(this Stream stream) => new ReadableStream(stream);

        private class ReadableStream : Readable
        {
            readonly Stream wrapped;

            public ReadableStream(Stream stream)
            {
                wrapped = stream;
            }

            public bool IsAtEnd() => false;

            public int Read(byte[] buffer, int offset, int count) => wrapped.Read(buffer, offset, count);

            public Readable SubReadable(int size)
            {
                throw new NotImplementedException();
            }
        }
    }
}
