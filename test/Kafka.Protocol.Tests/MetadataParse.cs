using Kafka.Protocol;
using System;
using System.IO;
using System.Linq;
using Xunit;
using static Kafka.Protocol.Tests.Helpers;

namespace Kafka.Protocol.Tests
{
    public class MetadataParse
    {
        [Fact]
        public void ParseMetadataV0() 
        {
            var binary = FromHex("0000000300000002002862726f6b6572322e6b61666b612e736f6d652e72616e646f6d2e636f72706f726174696f6e2e636f0000238400000001002862726f6b6572312e6b61666b612e736f6d652e72616e646f6d2e636f72706f726174696f6e2e636f0000238400000000002762726f6b65722e6b61666b612e736f6d652e72616e646f6d2e636f72706f726174696f6e2e636f000023840000000100000013536f6d6552616e646f6d546f7069634e616d6500000001000000000000000000010000000300000000000000010000000200000003000000010000000200000000");
            var stream = new MemoryStream(binary);
            var pstream = new ProtocolStreamReader(stream);

            var metadataResponse = Parse.MetadataResponse(0, pstream);
            var brokers = metadataResponse.Brokers.ToArray();
            Assert.Equal(3, brokers.Length);
            var broker0 = brokers[0];
            Assert.Equal(2, broker0.NodeId);
            Assert.Equal("broker2.kafka.some.random.corporation.co", broker0.HostName);
            Assert.Equal(9092, broker0.Port);
            var broker1 = brokers[1];
            Assert.Equal(1, broker1.NodeId);
            Assert.Equal("broker1.kafka.some.random.corporation.co", broker1.HostName);
            Assert.Equal(9092, broker1.Port);
            var broker2 = brokers[2];
            Assert.Equal(0, broker2.NodeId);
            Assert.Equal("broker.kafka.some.random.corporation.co", broker2.HostName);
            Assert.Equal(9092, broker2.Port);
            var topics = metadataResponse.Topics.ToArray();
            Assert.Equal(1, topics.Length);
            var topic0 = topics[0];
            Assert.Equal(0, topic0.TopicErrorCode);
            Assert.Equal("SomeRandomTopicName", topic0.TopicName);
            var topic0Partitions = topic0.Partitions.ToArray();
            Assert.Equal(1, topic0Partitions.Length);
            var topic0Partition0 = topic0Partitions[0];
            Assert.Equal(0, topic0Partition0.PartitionErrorCode);
            Assert.Equal(0, topic0Partition0.PartitionId);
            Assert.Equal(1, topic0Partition0.Leader);
            var replicas = topic0Partition0.Replicas.ToArray();
            Assert.Equal(new[] { 0, 1, 2 }, replicas);
            var isr = topic0Partition0.Isr.ToArray();
            Assert.Equal(new[] { 1, 2, 0 }, isr);
        }


    }
}
