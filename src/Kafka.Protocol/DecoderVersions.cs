using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Kafka.Protocol
{
    public class DecoderVersions<T>
    {
        public ApiKey ApiKey { get; }
        private Func<ProtocolStreamReader, T>[] ParseFuncs { get; }

        public DecoderVersions(ApiKey apiKey, params Func<ProtocolStreamReader, T>[] decodeFuncs)
        {
            ApiKey = apiKey;
            ParseFuncs = decodeFuncs;
        }

        public Func<ProtocolStreamReader, T> this[int version]
        {
            get
            {
                if (ApiKey != ApiKey.None && version < 0 || version >= ParseFuncs.Length)
                    throw new UnknownApiVersionException((short)version, ApiKey);
                return ParseFuncs[version];
            }
        }

        public T Decode(int version, ProtocolStreamReader reader) => this[version](reader);

        public int MaxVersion => ParseFuncs.Length - 1;
    }
}
