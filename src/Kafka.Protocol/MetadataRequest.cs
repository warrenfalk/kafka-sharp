using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Kafka.Protocol
{
    public class MetadataRequest : Request
    {
        public ApiKey ApiKey => Protocol.ApiKey.Metadata;
        public short ApiVersion { get; }
        public List<string> Topics { get; } = new List<string>();

        public MetadataRequest(short apiVersion)
        {
            ApiVersion = apiVersion;
        }

        public void WriteTo(ProtocolStreamWriter writer)
        {
            switch (ApiVersion)
            {
                case 0:
                case 1:
                case 2:
                    writer.WriteList(Topics, Encode.String);
                    break;
                default:
                    throw new UnknownApiVersionException(ApiVersion, ApiKey);
            }
        }
    }
}
