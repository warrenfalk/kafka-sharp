using Kafka.Protocol;
using System;
using System.IO;
using System.Linq;
using Xunit;
using static Kafka.Protocol.Tests.Helpers;

namespace Kafka.Protocol.Tests
{
    public class OffsetFetchRequestEncodeTests
    {
        [Fact]
        public void EncodeOffsetFetchV0()
        {
            var offsetFetchRequest = new OffsetFetchRequest(0)
            {
                GroupId = "test_group",
                Topics =
                {
                    new TopicOffsetFetch
                    {
                        TopicName = "Red",
                        Partitions =
                        {
                            new TopicPartitionOffsetFetch
                            {
                                Partition = 0,
                            }
                        }
                    }
                }
            };

            var stream = new MemoryStream();
            var writer = new RequestWriter(stream.AsWritable());
            writer.WriteRequest(offsetFetchRequest, 1, "test-client");
            var reqBytes = stream.ToArray();

            Assert.Equal(reqBytes, FromHex("000000320009000000000001000b746573742d636c69656e74000a746573745f67726f75700000000100035265640000000100000000"));
        }

        [Fact]
        public void EncodeOffsetFetchV1()
        {
            var offsetFetchRequest = new OffsetFetchRequest(1)
            {
                GroupId = "test_group",
                Topics =
                {
                    new TopicOffsetFetch
                    {
                        TopicName = "Red",
                        Partitions =
                        {
                            new TopicPartitionOffsetFetch
                            {
                                Partition = 0,
                            }
                        }
                    }
                }
            };

            var stream = new MemoryStream();
            var writer = new RequestWriter(stream.AsWritable());
            writer.WriteRequest(offsetFetchRequest, 1, "test-client");
            var reqBytes = stream.ToArray();

            Assert.Equal(reqBytes, FromHex("000000320009000100000001000b746573742d636c69656e74000a746573745f67726f75700000000100035265640000000100000000"));
        }

        [Fact]
        public void EncodeOffsetFetchBadVersion()
        {
            var version = (short)2;
            var offsetFetchRequest = new OffsetFetchRequest(version)
            {
                GroupId = "test_group",
                Topics =
                {
                    new TopicOffsetFetch
                    {
                        TopicName = "Red",
                        Partitions =
                        {
                            new TopicPartitionOffsetFetch
                            {
                                Partition = 0,
                            }
                        }
                    }
                }
            };

            var stream = new MemoryStream();
            var writer = new RequestWriter(stream.AsWritable());
            var ex = Assert.Throws<UnknownApiVersionException>(() =>
            {
                writer.WriteRequest(offsetFetchRequest, 1, "test-client");
            });
            Assert.Equal(ApiKey.OffsetFetch, ex.ApiKey);
            Assert.Equal(version, ex.ApiVersion);
        }

    }
}
