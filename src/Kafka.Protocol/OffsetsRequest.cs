using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Kafka.Protocol
{
    public class OffsetsRequest : Request
    {
        public ApiKey ApiKey => Protocol.ApiKey.Offsets;
        public short ApiVersion { get; }

        public int ReplicaId { get; set; } = -1;
        public List<TopicOffsets> Topics { get; } = new List<TopicOffsets>();

        public OffsetsRequest(short apiVersion)
        {
            ApiVersion = apiVersion;
        }

        public void WriteTo(ProtocolWriter writer)
        {
            switch (ApiVersion)
            {
                case 0:
                    writer
                        .WriteInt32(ReplicaId)
                        .WriteList(Topics, Protocol.TopicOffsets.EncodeV0);
                    break;
                case 1:
                    writer
                        .WriteInt32(ReplicaId)
                        .WriteList(Topics, Protocol.TopicOffsets.EncodeV1);
                    break;
                default:
                    throw new UnknownApiVersionException(ApiVersion, ApiKey);
            }
        }
    }

    public class TopicOffsets
    {
        public string TopicName { get; set; }
        public List<TopicPartitionOffsets> Partitions { get; } = new List<TopicPartitionOffsets>();

        public static ProtocolWriter EncodeV0(TopicOffsets value, ProtocolWriter writer)
            => writer
                .WriteString(value.TopicName)
                .WriteList(value.Partitions, TopicPartitionOffsets.EncodeV0);

        public static ProtocolWriter EncodeV1(TopicOffsets value, ProtocolWriter writer)
            => writer
                .WriteString(value.TopicName)
                .WriteList(value.Partitions, TopicPartitionOffsets.EncodeV1);
    }

    public class TopicPartitionOffsets
    {
        public int Partition { get; set; }
        public long Timestamp { get; set; }
        [Obsolete("Deprecated at version 0.10.1.0")]
        public int MaxNumOffsets { get; set; } // Not present after version 0

        public static ProtocolWriter EncodeV0(TopicPartitionOffsets value, ProtocolWriter writer)
            => writer
                .WriteInt32(value.Partition)
                .WriteInt64(value.Timestamp)
#pragma warning disable 0618
                .WriteInt32(value.MaxNumOffsets);
#pragma warning restore 0618

        public static ProtocolWriter EncodeV1(TopicPartitionOffsets value, ProtocolWriter writer)
            => writer
                .WriteInt32(value.Partition)
                .WriteInt64(value.Timestamp);
    }
}
