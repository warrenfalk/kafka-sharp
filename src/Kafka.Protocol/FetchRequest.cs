using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Kafka.Protocol
{
    public class FetchRequest : Request
    {
        public ApiKey ApiKey => Protocol.ApiKey.Fetch;
        public short ApiVersion { get; }
        public int ReplicaId { get; set; }
        public int MaxWaitTime { get; set; }
        public int MinBytes { get; set; }
        public int MaxBytes { get; set; }
        public List<TopicFetch> Topics { get; } = new List<TopicFetch>();

        public FetchRequest(short apiVersion)
        {
            ApiVersion = apiVersion;
        }

        public void WriteTo(ProtocolWriter writer)
        {
            switch (ApiVersion)
            {
                case 0:
                case 1:
                case 2:
                    writer.WriteInt32(ReplicaId);
                    writer.WriteInt32(MaxWaitTime);
                    writer.WriteInt32(MinBytes);
                    writer.WriteList(Topics, TopicFetch.Encode);
                    break;
                case 3:
                    writer.WriteInt32(ReplicaId);
                    writer.WriteInt32(MaxWaitTime);
                    writer.WriteInt32(MinBytes);
                    writer.WriteInt32(MaxBytes);
                    writer.WriteList(Topics, TopicFetch.Encode);
                    break;
                default:
                    throw new UnknownApiVersionException(ApiVersion, ApiKey);
            }
        }
    }

    public class TopicFetch
    {
        public string TopicName { get; set; }
        public List<TopicPartitionFetch> Partitions { get; } = new List<TopicPartitionFetch>();

        internal static ProtocolWriter Encode(TopicFetch value, ProtocolWriter writer)
            => writer
                .WriteString(value.TopicName)
                .WriteList(value.Partitions, TopicPartitionFetch.Encode);
    }

    public class TopicPartitionFetch
    {
        public int Partition { get; set; }
        public long FetchOffset { get; set; }
        public int MaxBytes { get; set; }

        internal static ProtocolWriter Encode(TopicPartitionFetch value, ProtocolWriter writer)
            => writer
                .WriteInt32(value.Partition)
                .WriteInt64(value.FetchOffset)
                .WriteInt32(value.MaxBytes);
    }
}
