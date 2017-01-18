using System;
using System.Collections.Generic;

namespace Kafka.Protocol
{
    public interface DescribeGroupsResponse
    {
        int Version { get; }
        IEnumerable<DescribeGroupResponse> Groups { get; }
    }

    public interface DescribeGroupResponse
    {
        short ErrorCode { get; }
        string GroupId { get; }
        string State { get; }
        string ProtocolType { get; }
        string Protocol { get; }
        IEnumerable<DescribeGroupMemberResponse> Members { get; }
    }

    public interface DescribeGroupMemberResponse
    {
        string MemberId { get; }
        string ClientId { get; }
        string ClientHost { get; }
        BinaryValue Metadata { get; }
        BinaryValue Assignment { get; }
    }

    class DescribeGroupsResponseImpl : DescribeGroupsResponse
    {
        public int Version { get; }
        public IEnumerable<DescribeGroupResponse> Groups { get; }

        public DescribeGroupsResponseImpl(
            int version,
            IEnumerable<DescribeGroupResponse> groups)
        {
            Version = version;
            Groups = groups;
        }

        public static DecoderVersions<DescribeGroupsResponse> Decode = new DecoderVersions<DescribeGroupsResponse>(
            ApiKey.DescribeGroups,
            reader => new DescribeGroupsResponseImpl(
                version: 0,
                groups: reader.ReadList(DescribeGroupResponseImpl.Versions[0])
            )
        );
    }

    class DescribeGroupResponseImpl : DescribeGroupResponse
    {
        public short ErrorCode { get; }
        public string GroupId { get; }
        public string State { get; }
        public string ProtocolType { get; }
        public string Protocol { get; }
        public IEnumerable<DescribeGroupMemberResponse> Members { get; }

        public DescribeGroupResponseImpl(
            short errorCode,
            string groupId,
            string state,
            string protocolType,
            string protocol,
            IEnumerable<DescribeGroupMemberResponse> members)
        {
            ErrorCode = errorCode;
            GroupId = groupId;
            State = state;
            ProtocolType = protocolType;
            Protocol = protocol;
            Members = members;
        }

        public static DecoderVersions<DescribeGroupResponse> Versions = new DecoderVersions<DescribeGroupResponse>(
            ApiKey.None,
            reader => new DescribeGroupResponseImpl(
                errorCode: reader.ReadInt16(),
                groupId: reader.ReadString(),
                state: reader.ReadString(),
                protocolType: reader.ReadString(),
                protocol: reader.ReadString(),
                members: reader.ReadList(DescribeGroupMemberResponseImpl.Versions[0])
            )
        );
    }

    class DescribeGroupMemberResponseImpl : DescribeGroupMemberResponse
    {
        public string MemberId { get; }
        public string ClientId { get; }
        public string ClientHost { get; }
        public BinaryValue Metadata { get; }
        public BinaryValue Assignment { get; }

        public DescribeGroupMemberResponseImpl(
            string memberId,
            string clientId,
            string clientHost,
            BinaryValue metadata,
            BinaryValue assignment)
        {
            MemberId = memberId;
            ClientId = clientId;
            ClientHost = clientHost;
            Metadata = metadata;
            Assignment = assignment;
        }

        public static DecoderVersions<DescribeGroupMemberResponse> Versions = new DecoderVersions<DescribeGroupMemberResponse>(
            ApiKey.None,
            reader => new DescribeGroupMemberResponseImpl(
                memberId: reader.ReadString(),
                clientId: reader.ReadString(),
                clientHost: reader.ReadString(),
                metadata: reader.ReadBytes(),
                assignment: reader.ReadBytes()
            )
        );
    }
}
