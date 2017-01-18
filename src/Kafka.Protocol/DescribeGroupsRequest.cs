using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Kafka.Protocol
{
    public class DescribeGroupsRequest : Request
    {
        public ApiKey ApiKey => Protocol.ApiKey.DescribeGroups;
        public short ApiVersion { get; }
        public List<string> GroupIds { get; } = new List<string>();

        public DescribeGroupsRequest(short apiVersion)
        {
            ApiVersion = apiVersion;
        }

        public void WriteTo(ProtocolWriter writer)
        {
            switch (ApiVersion)
            {
                case 0:
                    writer.WriteList(GroupIds, Encode.String);
                    break;
                default:
                    throw new UnknownApiVersionException(ApiVersion, ApiKey);
            }
        }
    }
}
