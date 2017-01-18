using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Kafka.Protocol
{
    public class ProduceRequest : Request
    {
        public ApiKey ApiKey => Protocol.ApiKey.Produce;
        public short ApiVersion { get; }

        public short Acks { get; set; }
        public int Timeout { get; set; }
        public List<TopicProduce> TopicData { get; } = new List<TopicProduce>();

        public ProduceRequest(short apiVersion)
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
                    writer.WriteInt16(Acks);
                    writer.WriteInt32(Timeout);
                    writer.WriteList(TopicData, Protocol.TopicProduce.Encode);
                    break;
                default:
                    throw new UnknownApiVersionException(ApiVersion, ApiKey);
            }
        }
    }

    public class TopicProduce
    {
        public string TopicName { get; set; }
        public List<TopicPartitionProduce> Data { get; } = new List<TopicPartitionProduce>();

        public static void Encode(TopicProduce value, ProtocolWriter writer)
        {
            writer.WriteString(value.TopicName);
            writer.WriteList(value.Data, TopicPartitionProduce.Encode);
        }
    }

    public class TopicPartitionProduce
    {
        public int Partition { get; set; }
        public MessageSet MessageSet { get; } = new MessageSet();

        public static void Encode(TopicPartitionProduce value, ProtocolWriter writer)
        {
            writer.WriteInt32(value.Partition);
            writer.WriteMessageSet(value.MessageSet);
        }
    }
}
