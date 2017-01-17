using Kafka.Protocol;
using System;
using System.IO;
using System.Linq;
using Xunit;
using static Kafka.Protocol.Tests.Helpers;

namespace Kafka.Protocol.Tests
{
    public class FetchDecode
    {
        [Fact]
        public void DecodeFetchV0() 
        {
            var binary = FromHex("000000630000000100000001000352656400000001000000000000000000000000000c000000400000000000000000000000143fe3b9f50000000000036f6e6500000003756e6f00000000000000010000001461f08f4100000000000374776f00000003646f73");
            var stream = new MemoryStream(binary);
            var pstream = new ProtocolStreamReader(stream);

            int size = pstream.ReadInt32();
            int correlationId = pstream.ReadInt32();

            var fetchResponse = Decode.FetchResponse(0, pstream);
            Assert.Equal(0, fetchResponse.Version);
            Assert.Equal(-1, fetchResponse.ThrottleTimeMs);
            var topics = fetchResponse.Topics.ToArray();
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
                    Assert.Equal(0, partition.HighWatermark);
                    //Assert.Equal(-1, partition.Timestamp);
                }
            }
        }

        /*
        [Fact]
        public void DecodeFetchV1()
        {
            var binary = FromHex("000000230000000100000001000352656400000001000000000000000000000000000400000000");
            var stream = new MemoryStream(binary);
            var pstream = new ProtocolStreamReader(stream);

            int size = pstream.ReadInt32();
            int correlationId = pstream.ReadInt32();

            var fetchResponse = Decode.FetchResponse(1, pstream);
            Assert.Equal(1, fetchResponse.Version);
            Assert.Equal(0, fetchResponse.ThrottleTimeMs);
            var topics = fetchResponse.Topics.ToArray();
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
        public void DecodeFetchV2()
        {
            var binary = FromHex("0000002b00000001000000010003526564000000010000000000000000000000000008ffffffffffffffff00000000");
            var stream = new MemoryStream(binary);
            var pstream = new ProtocolStreamReader(stream);

            int size = pstream.ReadInt32();
            int correlationId = pstream.ReadInt32();

            var fetchResponse = Decode.FetchResponse(2, pstream);
            Assert.Equal(2, fetchResponse.Version);
            Assert.Equal(0, fetchResponse.ThrottleTimeMs);
            var topics = fetchResponse.Topics.ToArray();
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
        public void DecodeFetchBadVersion()
        {
            var binary = FromHex("0000002b00000001000000010003526564000000010000000000000000000000000008ffffffffffffffff00000000");
            var stream = new MemoryStream(binary);
            var pstream = new ProtocolStreamReader(stream);

            int size = pstream.ReadInt32();
            int correlationId = pstream.ReadInt32();

            var badVersion = (short)3;
            var ex = Assert.Throws<UnknownApiVersionException>(() =>
            {
                var fetchResponse = Decode.FetchResponse(badVersion, pstream);
            });
            Assert.Equal(ApiKey.Fetch, ex.ApiKey);
            Assert.Equal(badVersion, ex.ApiVersion);
        }
        */
    }
}
