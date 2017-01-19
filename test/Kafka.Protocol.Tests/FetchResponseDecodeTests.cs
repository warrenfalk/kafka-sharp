using Kafka.Protocol;
using System;
using System.IO;
using System.Linq;
using Xunit;
using static Kafka.Protocol.Tests.Helpers;

namespace Kafka.Protocol.Tests
{
    public class FetchResponseDecodeTests
    {
        [Fact]
        public void DecodeFetchV0() 
        {
            var binary = FromHex("000000630000000100000001000352656400000001000000000000000000000000000c000000400000000000000000000000143fe3b9f50000000000036f6e6500000003756e6f00000000000000010000001461f08f4100000000000374776f00000003646f73");
            var pstream = new ProtocolReader(new Slice(binary));

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
                    Assert.Equal(KafkaError.None, partition.Error);
                    Assert.Equal(12, partition.HighWatermark);
                    var messageSet = partition.MessageSet.ToArray();
                    Assert.Equal(2, messageSet.Length);
                    {
                        var message = messageSet[0];
                        Assert.Equal(0, message.Offset);
                        Assert.Equal(0, message.Version);
                        Assert.Equal(Compression.None, message.Compression);
                        Assert.Equal("one", message.Key);
                        Assert.Equal("uno", message.Value);
                    }
                    {
                        var message = messageSet[1];
                        Assert.Equal(1, message.Offset);
                        Assert.Equal(0, message.Version);
                        Assert.Equal(Compression.None, message.Compression);
                        Assert.Equal("two", message.Key);
                        Assert.Equal("dos", message.Value);
                    }
                }
            }
        }

        [Fact]
        public void DecodeFetchV1()
        {
            var binary = FromHex("00000067000000010000000000000001000352656400000001000000000000000000000000000c000000400000000000000000000000143fe3b9f50000000000036f6e6500000003756e6f00000000000000010000001461f08f4100000000000374776f00000003646f73");
            var pstream = new ProtocolReader(new Slice(binary));

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
                    Assert.Equal(KafkaError.None, partition.Error);
                    Assert.Equal(12, partition.HighWatermark);
                    var messageSet = partition.MessageSet.ToArray();
                    Assert.Equal(2, messageSet.Length);
                    {
                        var message = messageSet[0];
                        Assert.Equal(0, message.Offset);
                        Assert.Equal(0, message.Version);
                        Assert.Equal(Compression.None, message.Compression);
                        Assert.Equal("one", message.Key);
                        Assert.Equal("uno", message.Value);
                    }
                    {
                        var message = messageSet[1];
                        Assert.Equal(1, message.Offset);
                        Assert.Equal(0, message.Version);
                        Assert.Equal(Compression.None, message.Compression);
                        Assert.Equal("two", message.Key);
                        Assert.Equal("dos", message.Value);
                    }
                }
            }
        }

        [Fact]
        public void DecodeFetchV2()
        {
            var binary = FromHex("00000087000000010000000000000001000352656400000001000000000000000000000000000c0000006000000000000000000000001c3e8e230a0100ffffffffffffffff000000036f6e6500000003756e6f00000000000000010000001c609d15be0100ffffffffffffffff0000000374776f00000003646f7300000000000000020000001f1cd792c3");
            var pstream = new ProtocolReader(new Slice(binary));

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
                    Assert.Equal(KafkaError.None, partition.Error);
                    Assert.Equal(12, partition.HighWatermark);
                    var messageSet = partition.MessageSet.ToArray();
                    Assert.Equal(2, messageSet.Length);
                    {
                        var message = messageSet[0];
                        Assert.Equal(0, message.Offset);
                        Assert.Equal(1, message.Version);
                        Assert.Equal(-1, message.Timestamp);
                        Assert.Equal(Compression.None, message.Compression);
                        Assert.Equal("one", message.Key);
                        Assert.Equal("uno", message.Value);
                    }
                    {
                        var message = messageSet[1];
                        Assert.Equal(1, message.Offset);
                        Assert.Equal(1, message.Version);
                        Assert.Equal(-1, message.Timestamp);
                        Assert.Equal(Compression.None, message.Compression);
                        Assert.Equal("two", message.Key);
                        Assert.Equal("dos", message.Value);
                    }
                }
            }
        }

        [Fact]
        public void DecodeFetchV3()
        {
            var binary = FromHex("00000087000000010000000000000001000352656400000001000000000000000000000000000c0000006000000000000000000000001c3e8e230a0100ffffffffffffffff000000036f6e6500000003756e6f00000000000000010000001c609d15be0100ffffffffffffffff0000000374776f00000003646f7300000000000000020000001f1cd792c3");
            var pstream = new ProtocolReader(new Slice(binary));

            int size = pstream.ReadInt32();
            int correlationId = pstream.ReadInt32();

            var fetchResponse = Decode.FetchResponse(3, pstream);
            Assert.Equal(3, fetchResponse.Version);
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
                    Assert.Equal(KafkaError.None, partition.Error);
                    Assert.Equal(12, partition.HighWatermark);
                    var messageSet = partition.MessageSet.ToArray();
                    Assert.Equal(2, messageSet.Length);
                    {
                        var message = messageSet[0];
                        Assert.Equal(0, message.Offset);
                        Assert.Equal(1, message.Version);
                        Assert.Equal(-1, message.Timestamp);
                        Assert.Equal(Compression.None, message.Compression);
                        Assert.Equal("one", message.Key);
                        Assert.Equal("uno", message.Value);
                    }
                    {
                        var message = messageSet[1];
                        Assert.Equal(1, message.Offset);
                        Assert.Equal(1, message.Version);
                        Assert.Equal(-1, message.Timestamp);
                        Assert.Equal(Compression.None, message.Compression);
                        Assert.Equal("two", message.Key);
                        Assert.Equal("dos", message.Value);
                    }
                }
            }
        }

        [Fact]
        public void DecodeFetchBadVersion()
        {
            var binary = FromHex("00000087000000010000000000000001000352656400000001000000000000000000000000000c0000006000000000000000000000001c3e8e230a0100ffffffffffffffff000000036f6e6500000003756e6f00000000000000010000001c609d15be0100ffffffffffffffff0000000374776f00000003646f7300000000000000020000001f1cd792c3");
            var pstream = new ProtocolReader(new Slice(binary));

            int size = pstream.ReadInt32();
            int correlationId = pstream.ReadInt32();

            var badVersion = (short)4;
            var ex = Assert.Throws<UnknownApiVersionException>(() =>
            {
                var fetchResponse = Decode.FetchResponse(badVersion, pstream);
            });
            Assert.Equal(ApiKey.Fetch, ex.ApiKey);
            Assert.Equal(badVersion, ex.ApiVersion);
        }
    }
}
