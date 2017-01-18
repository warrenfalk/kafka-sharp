using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Kafka.Protocol
{
    public class JoinGroupRequest : Request
    {
        public ApiKey ApiKey => Protocol.ApiKey.JoinGroup;
        public short ApiVersion { get; }

        public string GroupId { get; set; }
        public int SessionTimeout { get; set; }
        public int RebalanceTimeout { get; set; } // Since version 1
        public string MemberId { get; set; }
        public string ProtocolType { get; set; }
        public List<GroupProtocol> GroupProtocols { get; } = new List<GroupProtocol>();

        public JoinGroupRequest(short apiVersion)
        {
            ApiVersion = apiVersion;
        }

        public void WriteTo(ProtocolWriter writer)
        {
            switch (ApiVersion)
            {
                case 0:
                    writer
                        .WriteString(GroupId)
                        .WriteInt32(SessionTimeout)
                        .WriteString(MemberId)
                        .WriteString(ProtocolType)
                        .WriteList(GroupProtocols, Protocol.GroupProtocol.Encode);
                    break;
                case 1:
                    writer
                        .WriteString(GroupId)
                        .WriteInt32(SessionTimeout)
                        .WriteInt32(RebalanceTimeout)
                        .WriteString(MemberId)
                        .WriteString(ProtocolType)
                        .WriteList(GroupProtocols, Protocol.GroupProtocol.Encode);
                    break;
                default:
                    throw new UnknownApiVersionException(ApiVersion, ApiKey);
            }
        }
    }

    public class GroupProtocol
    {
        public string ProtocolName { get; set; }
        public BinaryValue ProtocolMetadata { get; set; }

        public static ProtocolWriter Encode(GroupProtocol value, ProtocolWriter writer)
            => writer
                .WriteString(value.ProtocolName)
                .WriteBytes(value.ProtocolMetadata);
    }
}
