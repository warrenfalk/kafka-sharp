using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Kafka.Protocol
{
    public class GroupCoordinatorRequest : Request
    {
        public ApiKey ApiKey => Protocol.ApiKey.GroupCoordinator;
        public short ApiVersion { get; }
        public string GroupId { get; set; }

        public GroupCoordinatorRequest(short apiVersion)
        {
            ApiVersion = apiVersion;
        }

        public void WriteTo(ProtocolWriter writer)
        {
            switch (ApiVersion)
            {
                case 0:
                    writer.WriteString(GroupId);
                    break;
                default:
                    throw new UnknownApiVersionException(ApiVersion, ApiKey);
            }
        }
    }
}
