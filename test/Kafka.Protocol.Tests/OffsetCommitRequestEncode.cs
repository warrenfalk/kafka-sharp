using Kafka.Protocol;
using System;
using System.IO;
using System.Linq;
using Xunit;
using static Kafka.Protocol.Tests.Helpers;

namespace Kafka.Protocol.Tests
{
    public class OffsetCommitRequestEncode
    {
        [Fact]
        public void EncodeOffsetCommitV0()
        {
            var offsetCommitRequest = new OffsetCommitRequest(0)
            {
                GroupId = "test_group",
                Topics =
                {
                    new TopicOffsetCommit
                    {
                        TopicName = "Red",
                        Partitions =
                        {
                            new TopicPartitionOffsetCommit
                            {
                                Partition = 0,
                                Offset = 8,
                                Metadata = "metadata",
                            }
                        }
                    }
                }
            };

            var stream = new MemoryStream();
            var writer = new RequestWriter(stream.AsWritable());
            writer.WriteRequest(offsetCommitRequest, 1, "test-client");
            var reqBytes = stream.ToArray();

            Assert.Equal(reqBytes, FromHex("000000440008000000000001000b746573742d636c69656e74000a746573745f67726f75700000000100035265640000000100000000000000000000000800086d65746164617461"));
        }

        [Fact]
        public void EncodeOffsetCommitV1()
        {
            var offsetCommitRequest = new OffsetCommitRequest(1)
            {
                GroupId = "test_group",
                GroupGenerationId = 1,
                MemberId = "one",
                Topics =
                {
                    new TopicOffsetCommit
                    {
                        TopicName = "Red",
                        Partitions =
                        {
                            new TopicPartitionOffsetCommit
                            {
                                Partition = 0,
                                Offset = 8,
#pragma warning disable 618
                                Timestamp = (long)(new DateTimeOffset(2017, 1, 18, 10, 0, 0, 0, TimeSpan.Zero) - new DateTimeOffset(1970, 1, 1, 0, 0, 0, TimeSpan.Zero)).TotalMilliseconds,
#pragma warning restore 618
                                Metadata = "metadata",
                            }
                        }
                    }
                }
            };

            var stream = new MemoryStream();
            var writer = new RequestWriter(stream.AsWritable());
            writer.WriteRequest(offsetCommitRequest, 1, "test-client");
            var reqBytes = stream.ToArray();

            Assert.Equal(reqBytes, FromHex("000000550008000100000001000b746573742d636c69656e74000a746573745f67726f75700000000100036f6e650000000100035265640000000100000000000000000000000800000159b104d10000086d65746164617461"));
        }

        [Fact]
        public void EncodeOffsetCommitV2()
        {
            var offsetCommitRequest = new OffsetCommitRequest(2)
            {
                GroupId = "test_group",
                GroupGenerationId = 1,
                MemberId = "one",
                RetentionTime = 60000,
                Topics =
                {
                    new TopicOffsetCommit
                    {
                        TopicName = "Red",
                        Partitions =
                        {`
                            new TopicPartitionOffsetCommit
                            {
                                Partition = 0,
                                Offset = 8,
                                Metadata = "metadata",
                            }
                        }
                    }
                }
            };

            var stream = new MemoryStream();
            var writer = new RequestWriter(stream.AsWritable());
            writer.WriteRequest(offsetCommitRequest, 1, "test-client");
            var reqBytes = stream.ToArray();

            Assert.Equal(reqBytes, FromHex("000000550008000200000001000b746573742d636c69656e74000a746573745f67726f75700000000100036f6e65000000000000ea600000000100035265640000000100000000000000000000000800086d65746164617461"));
        }

        [Fact]
        public void EncodeOffsetCommitBadVersion()
        {
            var version = (short)3;
            var offsetCommitRequest = new OffsetCommitRequest(version)
            {
                GroupId = "test_group",
                GroupGenerationId = 1,
                MemberId = "one",
                RetentionTime = 60000,
                Topics =
                {
                    new TopicOffsetCommit
                    {
                        TopicName = "Red",
                        Partitions =
                        {
                            new TopicPartitionOffsetCommit
                            {
                                Partition = 0,
                                Offset = 8,
                                Metadata = "metadata",
                            }
                        }
                    }
                }
            };

            var stream = new MemoryStream();
            var writer = new RequestWriter(stream.AsWritable());
            var ex = Assert.Throws<UnknownApiVersionException>(() =>
            {
                writer.WriteRequest(offsetCommitRequest, 1, "test-client");
            });
            Assert.Equal(ApiKey.OffsetCommit, ex.ApiKey);
            Assert.Equal(version, ex.ApiVersion);
        }

    }
}
