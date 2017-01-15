using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Kafka.Protocol
{
    public class ParserVersions<T>
    {
        private Func<ProtocolStreamReader, T>[] ParseFuncs { get; }

        public ParserVersions(params Func<ProtocolStreamReader, T>[] parseFuncs)
        {
            ParseFuncs = parseFuncs;
        }

        public Func<ProtocolStreamReader, T> this[int version] => ParseFuncs[version];

        public T Parse(int version, ProtocolStreamReader reader) => ParseFuncs[version](reader);

        public int MaxVersion => ParseFuncs.Length - 1;
    }
}
