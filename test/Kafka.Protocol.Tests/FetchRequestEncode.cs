using Kafka.Protocol;
using System;
using System.IO;
using System.Linq;
using Xunit;
using static Kafka.Protocol.Tests.Helpers;

namespace Kafka.Protocol.Tests
{
    public class FetchRequestEncode
    {
        [Fact]
        public void EncodeFetchV0()
        {
            var fetchRequest = new FetchRequest(0)
            {
                MaxWaitTime = 30,
                MinBytes = 1,
                Topics =
                {
                    new TopicFetch
                    {
                        TopicName = "Red",
                        Partitions =
                        {
                            new TopicPartitionFetch
                            {
                                Partition = 0,
                                FetchOffset = 0,
                                MaxBytes = 96,
                            }
                        }
                    }
                }
            };

            var stream = new MemoryStream();
            var writer = new RequestWriter(stream.AsWritable());
            writer.WriteRequest(fetchRequest, 1, "test-client");
            var reqBytes = stream.ToArray();

            Assert.Equal(reqBytes, FromHex("0000003e0001000000000001000b746573742d636c69656e74ffffffff0000001e000000010000000100035265640000000100000000000000000000000000000060"));
        }

        [Fact]
        public void EncodeFetchV1()
        {
            var fetchRequest = new FetchRequest(1)
            {
                MaxWaitTime = 30,
                MinBytes = 1,
                Topics =
                {
                    new TopicFetch
                    {
                        TopicName = "Red",
                        Partitions =
                        {
                            new TopicPartitionFetch
                            {
                                Partition = 0,
                                FetchOffset = 0,
                                MaxBytes = 96,
                            }
                        }
                    }
                }
            };

            var stream = new MemoryStream();
            var writer = new RequestWriter(stream.AsWritable());
            writer.WriteRequest(fetchRequest, 1, "test-client");
            var reqBytes = stream.ToArray();

            Assert.Equal(reqBytes, FromHex("0000003e0001000100000001000b746573742d636c69656e74ffffffff0000001e000000010000000100035265640000000100000000000000000000000000000060"));
        }

        [Fact]
        public void EncodeFetchV2()
        {
            var fetchRequest = new FetchRequest(2)
            {
                MaxWaitTime = 30,
                MinBytes = 1,
                Topics =
                {
                    new TopicFetch
                    {
                        TopicName = "Red",
                        Partitions =
                        {
                            new TopicPartitionFetch
                            {
                                Partition = 0,
                                FetchOffset = 0,
                                MaxBytes = 96,
                            }
                        }
                    }
                }
            };

            var stream = new MemoryStream();
            var writer = new RequestWriter(stream.AsWritable());
            writer.WriteRequest(fetchRequest, 1, "test-client");
            var reqBytes = stream.ToArray();

            Assert.Equal(reqBytes, FromHex("0000003e0001000200000001000b746573742d636c69656e74ffffffff0000001e000000010000000100035265640000000100000000000000000000000000000060"));
        }

        [Fact]
        public void EncodeFetchV3()
        {
            var fetchRequest = new FetchRequest(3)
            {
                MaxWaitTime = 30,
                MinBytes = 1,
                MaxBytes = 256,
                Topics =
                {
                    new TopicFetch
                    {
                        TopicName = "Red",
                        Partitions =
                        {
                            new TopicPartitionFetch
                            {
                                Partition = 0,
                                FetchOffset = 0,
                                MaxBytes = 96,
                            }
                        }
                    }
                }
            };

            var stream = new MemoryStream();
            var writer = new RequestWriter(stream.AsWritable());
            writer.WriteRequest(fetchRequest, 1, "test-client");
            var reqBytes = stream.ToArray();

            Assert.Equal(reqBytes, FromHex("000000420001000300000001000b746573742d636c69656e74ffffffff0000001e00000001000001000000000100035265640000000100000000000000000000000000000060"));
        }

        [Fact]
        public void EncodeFetchBadVersion()
        {
            var version = (short)4;
            var fetchRequest = new FetchRequest(version)
            {
                MaxWaitTime = 30,
                MinBytes = 1,
                Topics =
                {
                    new TopicFetch
                    {
                        TopicName = "Red",
                        Partitions =
                        {
                            new TopicPartitionFetch
                            {
                                Partition = 0,
                                FetchOffset = 0,
                                MaxBytes = 1024 * 1024,
                            }
                        }
                    }
                }
            };

            var stream = new MemoryStream();
            var writer = new RequestWriter(stream.AsWritable());
            var ex = Assert.Throws<UnknownApiVersionException>(() =>
            {
                writer.WriteRequest(fetchRequest, 1, "test-client");
            });
            Assert.Equal(ApiKey.Fetch, ex.ApiKey);
            Assert.Equal(version, ex.ApiVersion);
        }

    }
}
