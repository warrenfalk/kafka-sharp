using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Kafka.Protocol
{
    public class SyncGroupRequest : Request
    {
        public ApiKey ApiKey => Protocol.ApiKey.SyncGroup;
        public short ApiVersion { get; }

        public string GroupId { get; set; }
        public int GenerationId { get; set; }
        public string MemberId { get; set; }
        public List<GroupAssignment> GroupAssignments { get; } = new List<GroupAssignment>();

        public SyncGroupRequest(short apiVersion)
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
                        .WriteInt32(GenerationId)
                        .WriteString(MemberId)
                        .WriteList(GroupAssignments, Protocol.GroupAssignment.Encode);
                    break;
                default:
                    throw new UnknownApiVersionException(ApiVersion, ApiKey);
            }
        }
    }

    public class GroupAssignment
    {
        public string MemberId { get; set; }
        public BinaryValue MemberAssignment { get; set; }

        public static ProtocolWriter Encode(GroupAssignment value, ProtocolWriter writer)
            => writer
                .WriteString(value.MemberId)
                .WriteBytes(value.MemberAssignment);
    }
}
