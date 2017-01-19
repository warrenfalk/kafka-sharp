using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Kafka.Protocol
{
    public class DeleteTopicsRequest : Request
    {
        public ApiKey ApiKey => Protocol.ApiKey.DeleteTopics;
        public short ApiVersion { get; }

        public List<string> Topics { get; } = new List<string>();
        public int Timeout { get; set; }

        public DeleteTopicsRequest(short apiVersion)
        {
            ApiVersion = apiVersion;
        }

        public void WriteTo(ProtocolWriter writer)
        {
            switch (ApiVersion)
            {
                case 0:
                    writer
                        .WriteList(Topics, Protocol.Encode.String)
                        .WriteInt32(Timeout);
                    break;
                default:
                    throw new UnknownApiVersionException(ApiVersion, ApiKey);
            }
        }
    }
}
