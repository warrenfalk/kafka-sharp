using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Kafka.Protocol
{
    public class SaslHandshakeRequest : Request
    {
        public ApiKey ApiKey => Protocol.ApiKey.SaslHandshake;
        public short ApiVersion { get; }
        public string Mechanism { get; set; }

        public SaslHandshakeRequest(short apiVersion)
        {
            ApiVersion = apiVersion;
        }

        public void WriteTo(ProtocolWriter writer)
        {
            switch (ApiVersion)
            {
                case 0:
                    writer.WriteString(Mechanism);
                    break;
                default:
                    throw new UnknownApiVersionException(ApiVersion, ApiKey);
            }
        }
    }
}
