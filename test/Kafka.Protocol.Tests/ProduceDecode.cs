using Kafka.Protocol;
using System;
using System.IO;
using System.Linq;
using Xunit;
using static Kafka.Protocol.Tests.Helpers;

namespace Kafka.Protocol.Tests
{
    public class ProduceDecode
    {
        [Fact]
        public void DecodeProduceV0() 
        {
            var binary = FromHex("0000001f00000001000000010003526564000000010000000000000000000000000000");
            var stream = new MemoryStream(binary);
            var pstream = new ProtocolStreamReader(stream);

            int size = pstream.ReadInt32();
            int correlationId = pstream.ReadInt32();

            var produceResponse = Decode.ProduceResponse(0, pstream);
            Assert.Equal(0, produceResponse.Version);
            Assert.Equal(-1, produceResponse.ThrottleTimeMs);
            var topics = produceResponse.Topics.ToArray();
            Assert.Equal(1, topics.Length);
            {
                var topic = topics[0];
                Assert.Equal("Red", topic.TopicName);
                var partitions = topic.Partitions.ToArray();
                Assert.Equal(1, partitions.Length);
                {
                    var partition = partitions[0];
                    Assert.Equal(0, partition.Partition);
                    Assert.Equal(0, partition.ErrorCode);
                    Assert.Equal(0, partition.BaseOffset);
                    Assert.Equal(-1, partition.Timestamp);
                }
            }
        }

        [Fact]
        public void DecodeProduceV1()
        {
            var binary = FromHex("000000230000000100000001000352656400000001000000000000000000000000000400000000");
            var stream = new MemoryStream(binary);
            var pstream = new ProtocolStreamReader(stream);

            int size = pstream.ReadInt32();
            int correlationId = pstream.ReadInt32();

            var produceResponse = Decode.ProduceResponse(1, pstream);
            Assert.Equal(1, produceResponse.Version);
            Assert.Equal(0, produceResponse.ThrottleTimeMs);
            var topics = produceResponse.Topics.ToArray();
            Assert.Equal(1, topics.Length);
            {
                var topic = topics[0];
                Assert.Equal("Red", topic.TopicName);
                var partitions = topic.Partitions.ToArray();
                Assert.Equal(1, partitions.Length);
                {
                    var partition = partitions[0];
                    Assert.Equal(0, partition.Partition);
                    Assert.Equal(0, partition.ErrorCode);
                    Assert.Equal(4, partition.BaseOffset);
                    Assert.Equal(-1, partition.Timestamp);
                }
            }
        }

        [Fact]
        public void DecodeProduceV2()
        {
            var binary = FromHex("0000002b00000001000000010003526564000000010000000000000000000000000008ffffffffffffffff00000000");
            var stream = new MemoryStream(binary);
            var pstream = new ProtocolStreamReader(stream);

            int size = pstream.ReadInt32();
            int correlationId = pstream.ReadInt32();

            var produceResponse = Decode.ProduceResponse(2, pstream);
            Assert.Equal(2, produceResponse.Version);
            Assert.Equal(0, produceResponse.ThrottleTimeMs);
            var topics = produceResponse.Topics.ToArray();
            Assert.Equal(1, topics.Length);
            {
                var topic = topics[0];
                Assert.Equal("Red", topic.TopicName);
                var partitions = topic.Partitions.ToArray();
                Assert.Equal(1, partitions.Length);
                {
                    var partition = partitions[0];
                    Assert.Equal(0, partition.Partition);
                    Assert.Equal(0, partition.ErrorCode);
                    Assert.Equal(8, partition.BaseOffset);
                    Assert.Equal(-1, partition.Timestamp);
                }
            }
        }


        [Fact]
        public void DecodeProduceBadVersion()
        {
            var binary = FromHex("0000002b00000001000000010003526564000000010000000000000000000000000008ffffffffffffffff00000000");
            var stream = new MemoryStream(binary);
            var pstream = new ProtocolStreamReader(stream);

            int size = pstream.ReadInt32();
            int correlationId = pstream.ReadInt32();

            var badVersion = (short)3;
            var ex = Assert.Throws<UnknownApiVersionException>(() =>
            {
                var produceResponse = Decode.ProduceResponse(badVersion, pstream);
            });
            Assert.Equal(ApiKey.Produce, ex.ApiKey);
            Assert.Equal(badVersion, ex.ApiVersion);
        }

    }
}
