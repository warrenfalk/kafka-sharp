using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Kafka.Protocol
{
    public class DecoderVersions<T>
    {
        private Func<ProtocolStreamReader, T>[] ParseFuncs { get; }

        public DecoderVersions(params Func<ProtocolStreamReader, T>[] decodeFuncs)
        {
            ParseFuncs = decodeFuncs;
        }

        public Func<ProtocolStreamReader, T> this[int version] => ParseFuncs[version];

        public T Decode(int version, ProtocolStreamReader reader) => ParseFuncs[version](reader);

        public int MaxVersion => ParseFuncs.Length - 1;
    }
}
