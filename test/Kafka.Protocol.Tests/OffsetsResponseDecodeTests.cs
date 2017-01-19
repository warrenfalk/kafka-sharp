using Kafka.Protocol;
using System;
using System.IO;
using System.Linq;
using Xunit;
using static Kafka.Protocol.Tests.Helpers;

namespace Kafka.Protocol.Tests
{
    public class OffsetsResponseDecodeTests
    {
        [Fact]
        public void DecodeOffsetsV0() 
        {
            var binary = FromHex("00000023000000010000000100035265640000000100000000000000000001000000000000000c");
            var pstream = new ProtocolReader(new Slice(binary));

            int size = pstream.ReadInt32();
            int correlationId = pstream.ReadInt32();

            var offsetsResponse = Decode.OffsetsResponse(0, pstream);
            Assert.Equal(0, offsetsResponse.Version);
            var topics = offsetsResponse.Topics.ToArray();
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
                    Assert.Equal(12, partition.Offset);
                    Assert.Equal(-1, partition.Timestamp);
#pragma warning disable 618
                    Assert.Equal(new[] { 12L }, partition.Offsets);
#pragma warning restore 618
                }
            }
        }

        [Fact]
        public void DecodeOffsetsV1()
        {
            var binary = FromHex("000000270000000100000001000352656400000001000000000000ffffffffffffffff000000000000000c");
            var pstream = new ProtocolReader(new Slice(binary));

            int size = pstream.ReadInt32();
            int correlationId = pstream.ReadInt32();

            var offsetsResponse = Decode.OffsetsResponse(1, pstream);
            Assert.Equal(1, offsetsResponse.Version);
            var topics = offsetsResponse.Topics.ToArray();
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
                    Assert.Equal(12, partition.Offset);
                    Assert.Equal(-1, partition.Timestamp);
#pragma warning disable 618
                    Assert.Equal(new[] { 12L }, partition.Offsets);
#pragma warning restore 618
                }
            }
        }

        [Fact]
        public void DecodeOffsetsBadVersion()
        {
            var binary = FromHex("000000270000000100000001000352656400000001000000000000ffffffffffffffff000000000000000c");
            var pstream = new ProtocolReader(new Slice(binary));

            int size = pstream.ReadInt32();
            int correlationId = pstream.ReadInt32();

            var badVersion = (short)2;
            var ex = Assert.Throws<UnknownApiVersionException>(() =>
            {
                var offsetsResponse = Decode.OffsetsResponse(badVersion, pstream);
            });
            Assert.Equal(ApiKey.Offsets, ex.ApiKey);
            Assert.Equal(badVersion, ex.ApiVersion);
        }

    }
}
