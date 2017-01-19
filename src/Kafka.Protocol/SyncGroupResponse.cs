using System;
using System.Collections.Generic;

namespace Kafka.Protocol
{
    public interface SyncGroupResponse
    {
        int Version { get; }
        KafkaError Error { get; }
        BinaryValue MemberAssignment { get; }
    }

    class SyncGroupResponseImpl : SyncGroupResponse
    {
        public int Version { get; }
        public KafkaError Error { get; }
        public BinaryValue MemberAssignment { get; }

        public SyncGroupResponseImpl(
            int version,
            KafkaError error,
            BinaryValue memberAssignment)
        {
            Version = version;
            Error = error;
            MemberAssignment = memberAssignment;
        }

        public static DecoderVersions<SyncGroupResponse> Decode = new DecoderVersions<SyncGroupResponse>(
            ApiKey.SyncGroup,
            reader => new SyncGroupResponseImpl(
                version: 0,
                error: reader.ReadErrorCode(),
                memberAssignment: reader.ReadBytes()
            )
        );
    }
}
