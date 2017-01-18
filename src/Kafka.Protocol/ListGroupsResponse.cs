using System;
using System.Collections.Generic;

namespace Kafka.Protocol
{
    public interface ListGroupsResponse
    {
        int Version { get; }
        short ErrorCode { get; }
        IEnumerable<ListGroupResponse> Groups { get; }
    }

    public interface ListGroupResponse
    {
        string GroupId { get; }
        string ProtocolType { get; }
    }

    class ListGroupsResponseImpl : ListGroupsResponse
    {
        public int Version { get; }
        public short ErrorCode { get; }
        public IEnumerable<ListGroupResponse> Groups { get; }

        public ListGroupsResponseImpl(
            int version,
            short errorCode,
            IEnumerable<ListGroupResponse> groups)
        {
            Version = version;
            ErrorCode = errorCode;
            Groups = groups;
        }

        public static DecoderVersions<ListGroupsResponse> Versions = new DecoderVersions<ListGroupsResponse>(
            ApiKey.ListGroups,
            reader => new ListGroupsResponseImpl(
                version: 0,
                errorCode: reader.ReadInt16(),
                groups: reader.ReadList(ListGroupResponseImpl.Versions[0])
            )
        );
    }

    class ListGroupResponseImpl : ListGroupResponse
    {
        public string GroupId { get; }
        public string ProtocolType { get; }

        public ListGroupResponseImpl(
            string groupId,
            string protocolType)
        {
            GroupId = groupId;
            ProtocolType = protocolType;
        }

        public static DecoderVersions<ListGroupResponse> Versions = new DecoderVersions<ListGroupResponse>(
            ApiKey.None,
            reader => new ListGroupResponseImpl(
                groupId: reader.ReadString(),
                protocolType: reader.ReadString()
            )
        );
    }
}
