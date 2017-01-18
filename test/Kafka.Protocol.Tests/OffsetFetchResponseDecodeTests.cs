using Kafka.Protocol;
using System;
using System.IO;
using System.Linq;
using Xunit;
using static Kafka.Protocol.Tests.Helpers;

namespace Kafka.Protocol.Tests
{
    public class OffsetFetchResponseDecodeTests
    {
        [Fact]
        public void DecodeOffsetFetchV0() 
        {
            var binary = FromHex("00000021000000010000000100035265640000000100000000000000000000000800000000");
            var pstream = new ProtocolReader(new Slice(binary));

            int size = pstream.ReadInt32();
            int correlationId = pstream.ReadInt32();

            var offsetFetchResponse = Decode.OffsetFetchResponse(0, pstream);
            Assert.Equal(0, offsetFetchResponse.Version);
            var topics = offsetFetchResponse.Topics.ToArray();
            Assert.Equal(1, topics.Length);
            {
                var topic = topics[0];
                Assert.Equal("Red", topic.TopicName);
                var partitions = topic.Partitions.ToArray();
                Assert.Equal(1, partitions.Length);
                {
                    var partition = partitions[0];
                    Assert.Equal(0, partition.Partition);
                    Assert.Equal(8, partition.Offset);
                    Assert.Equal("", partition.Metadata);
                    Assert.Equal(0, partition.ErrorCode);
                }
            }
        }

        [Fact]
        public void DecodeOffsetFetchV1()
        {
            var binary = FromHex("00000021000000010000000100035265640000000100000000ffffffffffffffff00000010");
            var pstream = new ProtocolReader(new Slice(binary));

            int size = pstream.ReadInt32();
            int correlationId = pstream.ReadInt32();

            var offsetFetchResponse = Decode.OffsetFetchResponse(1, pstream);
            Assert.Equal(1, offsetFetchResponse.Version);
            var topics = offsetFetchResponse.Topics.ToArray();
            Assert.Equal(1, topics.Length);
            {
                var topic = topics[0];
                Assert.Equal("Red", topic.TopicName);
                var partitions = topic.Partitions.ToArray();
                Assert.Equal(1, partitions.Length);
                {
                    var partition = partitions[0];
                    Assert.Equal(0, partition.Partition);
                    Assert.Equal(-1, partition.Offset);
                    Assert.Equal("", partition.Metadata);
                    Assert.Equal(16, partition.ErrorCode);
                }
            }
        }


        [Fact]
        public void DecodeOffsetFetchBadVersion()
        {
            var binary = FromHex("0000002b00000001000000010003526564000000010000000000000000000000000008ffffffffffffffff00000000");
            var pstream = new ProtocolReader(new Slice(binary));

            int size = pstream.ReadInt32();
            int correlationId = pstream.ReadInt32();

            var badVersion = (short)2;
            var ex = Assert.Throws<UnknownApiVersionException>(() =>
            {
                var offsetFetchResponse = Decode.OffsetFetchResponse(badVersion, pstream);
            });
            Assert.Equal(ApiKey.OffsetFetch, ex.ApiKey);
            Assert.Equal(badVersion, ex.ApiVersion);
        }

    }
}
