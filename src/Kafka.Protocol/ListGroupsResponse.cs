using System;
using System.Collections.Generic;

namespace Kafka.Protocol
{
    public interface ListGroupsResponse
    {
        int Version { get; }
        KafkaError Error { get; }
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
        public KafkaError Error { get; }
        public IEnumerable<ListGroupResponse> Groups { get; }

        public ListGroupsResponseImpl(
            int version,
            KafkaError error,
            IEnumerable<ListGroupResponse> groups)
        {
            Version = version;
            Error = error;
            Groups = groups;
        }

        public static DecoderVersions<ListGroupsResponse> Versions = new DecoderVersions<ListGroupsResponse>(
            ApiKey.ListGroups,
            reader => new ListGroupsResponseImpl(
                version: 0,
                error: reader.ReadErrorCode(),
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
