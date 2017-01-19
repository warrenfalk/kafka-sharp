using System;
using System.Collections.Generic;

namespace Kafka.Protocol
{
    public interface JoinGroupResponse
    {
        int Version { get; }
        KafkaError Error { get; }
        int GenerationId { get; }
        string GroupProtocol { get; }
        string LeaderId { get; }
        string MemberId { get; }
        IEnumerable<JoinGroupMemberResponse> Members { get; }
    }

    public interface JoinGroupMemberResponse
    {
        string MemberId { get; }
        BinaryValue MemberMetadata { get; }
    }

    class JoinGroupResponseImpl : JoinGroupResponse
    {
        public int Version { get; }
        public KafkaError Error { get; }
        public int GenerationId { get; }
        public string GroupProtocol { get; }
        public string LeaderId { get; }
        public string MemberId { get; }
        public IEnumerable<JoinGroupMemberResponse> Members { get; }

        public JoinGroupResponseImpl(
            int version,
            KafkaError error,
            int generationId,
            string groupProtocol,
            string leaderId,
            string memberId,
            IEnumerable<JoinGroupMemberResponse> members)
        {
            Version = version;
            Error = error;
            GenerationId = generationId;
            GroupProtocol = groupProtocol;
            LeaderId = leaderId;
            MemberId = memberId;
            Members = members;
        }

        public static DecoderVersions<JoinGroupResponse> Decode = new DecoderVersions<JoinGroupResponse>(
            ApiKey.JoinGroup,
            reader => new JoinGroupResponseImpl(
                version: 0,
                error: reader.ReadErrorCode(),
                generationId: reader.ReadInt32(),
                groupProtocol: reader.ReadString(),
                leaderId: reader.ReadString(),
                memberId: reader.ReadString(),
                members: reader.ReadList(JoinGroupMemberResponseImpl.Versions[0])
            ),
            reader => new JoinGroupResponseImpl(
                version: 1,
                error: reader.ReadErrorCode(),
                generationId: reader.ReadInt32(),
                groupProtocol: reader.ReadString(),
                leaderId: reader.ReadString(),
                memberId: reader.ReadString(),
                members: reader.ReadList(JoinGroupMemberResponseImpl.Versions[0])
            )
        );
    }

    class JoinGroupMemberResponseImpl : JoinGroupMemberResponse
    {
        public string MemberId { get; }
        public BinaryValue MemberMetadata { get; }

        public JoinGroupMemberResponseImpl(
            string memberId,
            BinaryValue memberMetadata)
        {
            MemberId = memberId;
            MemberMetadata = memberMetadata;
        }

        public static DecoderVersions<JoinGroupMemberResponse> Versions = new DecoderVersions<JoinGroupMemberResponse>(
            ApiKey.None,
            reader => new JoinGroupMemberResponseImpl(
                memberId: reader.ReadString(),
                memberMetadata: reader.ReadBytes()
            )
        );
    }
}
