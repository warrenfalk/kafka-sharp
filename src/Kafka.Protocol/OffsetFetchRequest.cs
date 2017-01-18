using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Kafka.Protocol
{
    public class OffsetFetchRequest : Request
    {
        public ApiKey ApiKey => Protocol.ApiKey.OffsetFetch;
        public short ApiVersion { get; }

        public string GroupId { get; set; }
        public List<TopicOffsetFetch> Topics { get; } = new List<TopicOffsetFetch>();

        public OffsetFetchRequest(short apiVersion)
        {
            ApiVersion = apiVersion;
        }

        public void WriteTo(ProtocolWriter writer)
        {
            switch (ApiVersion)
            {
                case 0:
                case 1:
                    writer
                        .WriteString(GroupId)
                        .WriteList(Topics, Protocol.TopicOffsetFetch.EncodeV0);
                    break;
                default:
                    throw new UnknownApiVersionException(ApiVersion, ApiKey);
            }
        }
    }

    public class TopicOffsetFetch
    {
        public string TopicName { get; set; }
        public List<TopicPartitionOffsetFetch> Partitions { get; } = new List<TopicPartitionOffsetFetch>();

        public static ProtocolWriter EncodeV0(TopicOffsetFetch value, ProtocolWriter writer)
            => writer
                .WriteString(value.TopicName)
                .WriteList(value.Partitions, TopicPartitionOffsetFetch.EncodeV0);
    }

    public class TopicPartitionOffsetFetch
    {
        public int Partition { get; set; }

        public static ProtocolWriter EncodeV0(TopicPartitionOffsetFetch value, ProtocolWriter writer)
            => writer
                .WriteInt32(value.Partition);

    }
}
