using Kafka.Protocol;
using System;
using System.IO;
using System.Linq;
using Xunit;
using static Kafka.Protocol.Tests.Helpers;

namespace Kafka.Protocol.Tests
{
    public class OffsetsRequestEncode
    {
        [Fact]
        public void EncodeOffsetsV0()
        {
            var offsetsRequest = new OffsetsRequest(0)
            {
                Topics =
                {
                    new TopicOffsets
                    {
                        TopicName = "Red",
                        Partitions =
                        {
                            new TopicPartitionOffsets
                            {
                                Partition = 0,
                                Timestamp = -1,
#pragma warning disable 0618
                                MaxNumOffsets = 1,
#pragma warning restore 0618
                            },
                        },
                    },
                },
            };

            var stream = new MemoryStream();
            var writer = new RequestWriter(stream.AsWritable());
            writer.WriteRequest(offsetsRequest, 1, "test-client");
            var reqBytes = stream.ToArray();

            Assert.Equal(reqBytes, FromHex("000000360002000000000001000b746573742d636c69656e74ffffffff0000000100035265640000000100000000ffffffffffffffff00000001"));
        }

        [Fact]
        public void EncodeOffsetsV1()
        {
            var offsetsRequest = new OffsetsRequest(1)
            {
                Topics =
                {
                    new TopicOffsets
                    {
                        TopicName = "Red",
                        Partitions =
                        {
                            new TopicPartitionOffsets
                            {
                                Partition = 0,
                                Timestamp = -1,
                            },
                        },
                    },
                },
            };

            var stream = new MemoryStream();
            var writer = new RequestWriter(stream.AsWritable());
            writer.WriteRequest(offsetsRequest, 1, "test-client");
            var reqBytes = stream.ToArray();

            Assert.Equal(reqBytes, FromHex("000000320002000100000001000b746573742d636c69656e74ffffffff0000000100035265640000000100000000ffffffffffffffff"));
        }

        [Fact]
        public void EncodeOffsetsBadVersion()
        {
            var version = (short)2;
            var offsetsRequest = new OffsetsRequest(version)
            {
                Topics =
                {
                    new TopicOffsets
                    {
                        TopicName = "Red",
                        Partitions =
                        {
                            new TopicPartitionOffsets
                            {
                                Partition = 0,
                                Timestamp = 0,
                            },
                        },
                    },
                },
            };

            var stream = new MemoryStream();
            var writer = new RequestWriter(stream.AsWritable());
            var ex = Assert.Throws<UnknownApiVersionException>(() =>
            {
                writer.WriteRequest(offsetsRequest, 1, "test-client");
            });
            Assert.Equal(ApiKey.Offsets, ex.ApiKey);
            Assert.Equal(version, ex.ApiVersion);
        }

    }
}
