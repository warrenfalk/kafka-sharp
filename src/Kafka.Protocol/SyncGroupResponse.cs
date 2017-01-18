using System;
using System.Collections.Generic;

namespace Kafka.Protocol
{
    public interface SyncGroupResponse
    {
        int Version { get; }
        short ErrorCode { get; }
        BinaryValue MemberAssignment { get; }
    }

    class SyncGroupResponseImpl : SyncGroupResponse
    {
        public int Version { get; }
        public short ErrorCode { get; }
        public BinaryValue MemberAssignment { get; }

        public SyncGroupResponseImpl(
            int version,
            short errorCode,
            BinaryValue memberAssignment)
        {
            Version = version;
            ErrorCode = errorCode;
            MemberAssignment = memberAssignment;
        }

        public static DecoderVersions<SyncGroupResponse> Decode = new DecoderVersions<SyncGroupResponse>(
            ApiKey.SyncGroup,
            reader => new SyncGroupResponseImpl(
                version: 0,
                errorCode: reader.ReadInt16(),
                memberAssignment: reader.ReadBytes()
            )
        );
    }
}
