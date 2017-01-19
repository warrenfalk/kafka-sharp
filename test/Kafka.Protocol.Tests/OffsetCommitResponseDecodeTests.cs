using Kafka.Protocol;
using System;
using System.IO;
using System.Linq;
using Xunit;
using static Kafka.Protocol.Tests.Helpers;

namespace Kafka.Protocol.Tests
{
    public class OffsetCommitResponseDecodeTests
    {
        [Fact]
        public void DecodeOffsetCommitV0() 
        {
            var binary = FromHex("000000170000000100000001000352656400000001000000000000");
            var pstream = new ProtocolReader(new Slice(binary));

            int size = pstream.ReadInt32();
            int correlationId = pstream.ReadInt32();

            var offsetCommitResponse = Decode.OffsetCommitResponse(0, pstream);
            Assert.Equal(0, offsetCommitResponse.Version);
            var topics = offsetCommitResponse.Topics.ToArray();
            Assert.Equal(1, topics.Length);
            {
                var topic = topics[0];
                Assert.Equal("Red", topic.TopicName);
                var partitions = topic.Partitions.ToArray();
                Assert.Equal(1, partitions.Length);
                {
                    var partition = partitions[0];
                    Assert.Equal(0, partition.Partition);
                    Assert.Equal(KafkaError.None, partition.Error);
                }
            }
        }

        [Fact]
        public void DecodeOffsetCommitV1()
        {
            var binary = FromHex("000000170000000100000001000352656400000001000000000010");
            var pstream = new ProtocolReader(new Slice(binary));

            int size = pstream.ReadInt32();
            int correlationId = pstream.ReadInt32();

            var offsetCommitResponse = Decode.OffsetCommitResponse(1, pstream);
            Assert.Equal(1, offsetCommitResponse.Version);
            var topics = offsetCommitResponse.Topics.ToArray();
            Assert.Equal(1, topics.Length);
            {
                var topic = topics[0];
                Assert.Equal("Red", topic.TopicName);
                var partitions = topic.Partitions.ToArray();
                Assert.Equal(1, partitions.Length);
                {
                    var partition = partitions[0];
                    Assert.Equal(0, partition.Partition);
                    Assert.Equal(KafkaError.NotCoordinatorForGroup, partition.Error);
                }
            }
        }

        [Fact]
        public void DecodeOffsetCommitV2()
        {
            var binary = FromHex("000000170000000100000001000352656400000001000000000010");
            var pstream = new ProtocolReader(new Slice(binary));

            int size = pstream.ReadInt32();
            int correlationId = pstream.ReadInt32();

            var offsetCommitResponse = Decode.OffsetCommitResponse(2, pstream);
            Assert.Equal(2, offsetCommitResponse.Version);
            var topics = offsetCommitResponse.Topics.ToArray();
            Assert.Equal(1, topics.Length);
            {
                var topic = topics[0];
                Assert.Equal("Red", topic.TopicName);
                var partitions = topic.Partitions.ToArray();
                Assert.Equal(1, partitions.Length);
                {
                    var partition = partitions[0];
                    Assert.Equal(0, partition.Partition);
                    Assert.Equal(KafkaError.NotCoordinatorForGroup, partition.Error);
                }
            }
        }


        [Fact]
        public void DecodeOffsetCommitBadVersion()
        {
            var binary = FromHex("0000002b00000001000000010003526564000000010000000000000000000000000008ffffffffffffffff00000000");
            var pstream = new ProtocolReader(new Slice(binary));

            int size = pstream.ReadInt32();
            int correlationId = pstream.ReadInt32();

            var badVersion = (short)3;
            var ex = Assert.Throws<UnknownApiVersionException>(() =>
            {
                var offsetCommitResponse = Decode.OffsetCommitResponse(badVersion, pstream);
            });
            Assert.Equal(ApiKey.OffsetCommit, ex.ApiKey);
            Assert.Equal(badVersion, ex.ApiVersion);
        }

    }
}
