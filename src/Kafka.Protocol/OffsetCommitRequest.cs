using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Kafka.Protocol
{
    public class OffsetCommitRequest : Request
    {
        public ApiKey ApiKey => Protocol.ApiKey.OffsetCommit;
        public short ApiVersion { get; }

        public string GroupId { get; set; }
        public int GroupGenerationId { get; set; } // Since version 1
        public string MemberId { get; set; } // Since version 1
        public long RetentionTime { get; set; } // Since version 2
        public List<TopicOffsetCommit> Topics { get; } = new List<TopicOffsetCommit>();

        public OffsetCommitRequest(short apiVersion)
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
                        .WriteList(Topics, Protocol.TopicOffsetCommit.EncodeV0);
                    break;
                case 1:
                    writer
                        .WriteString(GroupId)
                        .WriteInt32(GroupGenerationId)
                        .WriteString(MemberId)
                        .WriteList(Topics, Protocol.TopicOffsetCommit.EncodeV1);
                    break;
                case 2:
                    writer
                        .WriteString(GroupId)
                        .WriteInt32(GroupGenerationId)
                        .WriteString(MemberId)
                        .WriteInt64(RetentionTime)
                        .WriteList(Topics, Protocol.TopicOffsetCommit.EncodeV0);
                    break;
                default:
                    throw new UnknownApiVersionException(ApiVersion, ApiKey);
            }
        }
    }

    public class TopicOffsetCommit
    {
        public string TopicName { get; set; }
        public List<TopicPartitionOffsetCommit> Partitions { get; } = new List<TopicPartitionOffsetCommit>();

        public static ProtocolWriter EncodeV0(TopicOffsetCommit value, ProtocolWriter writer)
            => writer
                .WriteString(value.TopicName)
                .WriteList(value.Partitions, TopicPartitionOffsetCommit.EncodeV0);
        public static ProtocolWriter EncodeV1(TopicOffsetCommit value, ProtocolWriter writer)
            => writer
                .WriteString(value.TopicName)
                .WriteList(value.Partitions, TopicPartitionOffsetCommit.EncodeV1);
    }

    public class TopicPartitionOffsetCommit
    {
        public int Partition { get; set; }
        public long Offset { get; set; }
        [Obsolete("Removed in 0.9.0")]
        public long Timestamp { get; set; } // Added in version 1, Gone in version 2
        public string Metadata { get; set; }

        public static ProtocolWriter EncodeV0(TopicPartitionOffsetCommit value, ProtocolWriter writer)
            => writer
                .WriteInt32(value.Partition)
                .WriteInt64(value.Offset)
                .WriteString(value.Metadata);

        public static ProtocolWriter EncodeV1(TopicPartitionOffsetCommit value, ProtocolWriter writer)
            => writer
                .WriteInt32(value.Partition)
                .WriteInt64(value.Offset)
#pragma warning disable 618
                .WriteInt64(value.Timestamp)
#pragma warning restore 618
                .WriteString(value.Metadata);
    }
}
