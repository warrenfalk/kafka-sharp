using Kafka.Protocol;
using System;
using System.IO;
using System.Linq;
using Xunit;
using static Kafka.Protocol.Tests.Helpers;

namespace Kafka.Protocol.Tests
{
    public class MetadataDecode
    {
        [Fact]
        public void DecodeMetadataV0() 
        {
            var binary = FromHex("0000006f000000010000000100000000000e77617272656e2d6465736b746f7000002384000000020000000352656400000001000000000000000000000000000100000000000000010000000000000004426c7565000000010000000000000000000000000001000000000000000100000000");
            var stream = new MemoryStream(binary);
            var pstream = new ProtocolStreamReader(stream);

            int size = pstream.ReadInt32();
            int correlationId = pstream.ReadInt32();

            var metadataResponse = Decode.MetadataResponse(0, pstream);
            Assert.Equal(0, metadataResponse.Version);
            var brokers = metadataResponse.Brokers.ToArray();
            Assert.Equal(1, brokers.Length);
            {
                var broker = brokers[0];
                Assert.Equal(0, broker.NodeId);
                Assert.Equal("warren-desktop", broker.HostName);
                Assert.Equal(9092, broker.Port);
                Assert.Equal(null, broker.Rack);
            }
            Assert.Equal(null, metadataResponse.ClusterId);
            Assert.Equal(-1, metadataResponse.ControllerId);
            var topics = metadataResponse.Topics.ToArray();
            Assert.Equal(2, topics.Length);
            {
                var topic = topics[0];
                Assert.Equal(0, topic.TopicErrorCode);
                Assert.Equal("Red", topic.TopicName);
                Assert.Equal(false, topic.IsInternal);
                var topicPartitions = topic.Partitions.ToArray();
                Assert.Equal(1, topicPartitions.Length);
                var topicPartition = topicPartitions[0];
                Assert.Equal(0, topicPartition.PartitionErrorCode);
                Assert.Equal(0, topicPartition.PartitionId);
                Assert.Equal(0, topicPartition.Leader);
                var replicas = topicPartition.Replicas.ToArray();
                Assert.Equal(new[] { 0 }, replicas);
                var isr = topicPartition.Isr.ToArray();
                Assert.Equal(new[] { 0 }, isr);
            }
            {
                var topic = topics[1];
                Assert.Equal(0, topic.TopicErrorCode);
                Assert.Equal("Blue", topic.TopicName);
                Assert.Equal(false, topic.IsInternal);
                var topicPartitions = topic.Partitions.ToArray();
                Assert.Equal(1, topicPartitions.Length);
                var topicPartition = topicPartitions[0];
                Assert.Equal(0, topicPartition.PartitionErrorCode);
                Assert.Equal(0, topicPartition.PartitionId);
                Assert.Equal(0, topicPartition.Leader);
                var replicas = topicPartition.Replicas.ToArray();
                Assert.Equal(new[] { 0 }, replicas);
                var isr = topicPartition.Isr.ToArray();
                Assert.Equal(new[] { 0 }, isr);
            }
        }

        [Fact]
        public void DecodeMetadataV1()
        {
            var binary = FromHex("00000077000000010000000100000000000e77617272656e2d6465736b746f7000002384ffff0000000000000002000000035265640000000001000000000000000000000000000100000000000000010000000000000004426c756500000000010000000000000000000000000001000000000000000100000000");
            var stream = new MemoryStream(binary);
            var pstream = new ProtocolStreamReader(stream);

            int size = pstream.ReadInt32();
            int correlationId = pstream.ReadInt32();

            var metadataResponse = Decode.MetadataResponse(1, pstream);
            Assert.Equal(1, metadataResponse.Version);
            var brokers = metadataResponse.Brokers.ToArray();
            Assert.Equal(1, brokers.Length);
            {
                var broker = brokers[0];
                Assert.Equal(0, broker.NodeId);
                Assert.Equal("warren-desktop", broker.HostName);
                Assert.Equal(9092, broker.Port);
                Assert.Equal(null, broker.Rack);
            }
            Assert.Equal(null, metadataResponse.ClusterId);
            Assert.Equal(0, metadataResponse.ControllerId);
            var topics = metadataResponse.Topics.ToArray();
            Assert.Equal(2, topics.Length);
            {
                var topic = topics[0];
                Assert.Equal(0, topic.TopicErrorCode);
                Assert.Equal("Red", topic.TopicName);
                Assert.Equal(false, topic.IsInternal);
                var topicPartitions = topic.Partitions.ToArray();
                Assert.Equal(1, topicPartitions.Length);
                var topicPartition = topicPartitions[0];
                Assert.Equal(0, topicPartition.PartitionErrorCode);
                Assert.Equal(0, topicPartition.PartitionId);
                Assert.Equal(0, topicPartition.Leader);
                var replicas = topicPartition.Replicas.ToArray();
                Assert.Equal(new[] { 0 }, replicas);
                var isr = topicPartition.Isr.ToArray();
                Assert.Equal(new[] { 0 }, isr);
            }
            {
                var topic = topics[1];
                Assert.Equal(0, topic.TopicErrorCode);
                Assert.Equal("Blue", topic.TopicName);
                Assert.Equal(false, topic.IsInternal);
                var topicPartitions = topic.Partitions.ToArray();
                Assert.Equal(1, topicPartitions.Length);
                var topicPartition = topicPartitions[0];
                Assert.Equal(0, topicPartition.PartitionErrorCode);
                Assert.Equal(0, topicPartition.PartitionId);
                Assert.Equal(0, topicPartition.Leader);
                var replicas = topicPartition.Replicas.ToArray();
                Assert.Equal(new[] { 0 }, replicas);
                var isr = topicPartition.Isr.ToArray();
                Assert.Equal(new[] { 0 }, isr);
            }
        }

        [Fact]
        public void DecodeMetadataV2()
        {
            var binary = FromHex("0000008f000000010000000100000000000e77617272656e2d6465736b746f7000002384ffff00164553565f42776b615153326e7956776f51586d4553670000000000000002000000035265640000000001000000000000000000000000000100000000000000010000000000000004426c756500000000010000000000000000000000000001000000000000000100000000");
            var stream = new MemoryStream(binary);
            var pstream = new ProtocolStreamReader(stream);

            int size = pstream.ReadInt32();
            int correlationId = pstream.ReadInt32();

            var metadataResponse = Decode.MetadataResponse(2, pstream);
            Assert.Equal(2, metadataResponse.Version);
            var brokers = metadataResponse.Brokers.ToArray();
            Assert.Equal(1, brokers.Length);
            {
                var broker = brokers[0];
                Assert.Equal(0, broker.NodeId);
                Assert.Equal("warren-desktop", broker.HostName);
                Assert.Equal(9092, broker.Port);
                Assert.Equal(null, broker.Rack);
            }
            Assert.Equal("ESV_BwkaQS2nyVwoQXmESg", metadataResponse.ClusterId);
            Assert.Equal(0, metadataResponse.ControllerId);
            var topics = metadataResponse.Topics.ToArray();
            Assert.Equal(2, topics.Length);
            {
                var topic = topics[0];
                Assert.Equal(0, topic.TopicErrorCode);
                Assert.Equal("Red", topic.TopicName);
                Assert.Equal(false, topic.IsInternal);
                var topicPartitions = topic.Partitions.ToArray();
                Assert.Equal(1, topicPartitions.Length);
                var topicPartition = topicPartitions[0];
                Assert.Equal(0, topicPartition.PartitionErrorCode);
                Assert.Equal(0, topicPartition.PartitionId);
                Assert.Equal(0, topicPartition.Leader);
                var replicas = topicPartition.Replicas.ToArray();
                Assert.Equal(new[] { 0 }, replicas);
                var isr = topicPartition.Isr.ToArray();
                Assert.Equal(new[] { 0 }, isr);
            }
            {
                var topic = topics[1];
                Assert.Equal(0, topic.TopicErrorCode);
                Assert.Equal("Blue", topic.TopicName);
                Assert.Equal(false, topic.IsInternal);
                var topicPartitions = topic.Partitions.ToArray();
                Assert.Equal(1, topicPartitions.Length);
                var topicPartition = topicPartitions[0];
                Assert.Equal(0, topicPartition.PartitionErrorCode);
                Assert.Equal(0, topicPartition.PartitionId);
                Assert.Equal(0, topicPartition.Leader);
                var replicas = topicPartition.Replicas.ToArray();
                Assert.Equal(new[] { 0 }, replicas);
                var isr = topicPartition.Isr.ToArray();
                Assert.Equal(new[] { 0 }, isr);
            }
        }


        [Fact]
        public void DecodeMetadataBadVersion()
        {
            var binary = FromHex("0000008f000000010000000100000000000e77617272656e2d6465736b746f7000002384ffff00164553565f42776b615153326e7956776f51586d4553670000000000000002000000035265640000000001000000000000000000000000000100000000000000010000000000000004426c756500000000010000000000000000000000000001000000000000000100000000");
            var stream = new MemoryStream(binary);
            var pstream = new ProtocolStreamReader(stream);

            int size = pstream.ReadInt32();
            int correlationId = pstream.ReadInt32();

            var badVersion = (short)3;
            var ex = Assert.Throws<UnknownApiVersionException>(() =>
            {
                var metadataResponse = Decode.MetadataResponse(badVersion, pstream);
            });
            Assert.Equal(ApiKey.Metadata, ex.ApiKey);
            Assert.Equal(badVersion, ex.ApiVersion);
        }

    }
}
