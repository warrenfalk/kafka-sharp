using Kafka.Protocol;
using System;
using System.IO;
using System.Linq;
using Xunit;
using static Kafka.Protocol.Tests.Helpers;

namespace Kafka.Protocol.Tests
{
    public class ProduceRequestEncodeTests
    {
        [Fact]
        public void EncodeProduceV0()
        {
            var produceRequest = new ProduceRequest(0)
            {
                Acks = 1,
                Timeout = 60000,
                TopicData =
                {
                    new TopicProduce
                    {
                        TopicName = "Red",
                        Data =
                        {
                            new TopicPartitionProduce
                            {
                                Partition = 0,
                                MessageSet =
                                {
                                    { "one", "uno" },
                                    { "two", "dos" },
                                    { "three", "tres" },
                                    { "four", "quatro" },
                                },
                            },
                        },
                    },
                },
            };

            var stream = new MemoryStream();
            var writer = new RequestWriter(stream.AsWritable());
            writer.WriteRequest(produceRequest, 1, "test-client");
            var reqBytes = stream.ToArray();

            Assert.Equal(reqBytes, FromHex("000000b70000000000000001000b746573742d636c69656e7400010000ea600000000100035265640000000100000000000000870000000000000000000000143fe3b9f50000000000036f6e6500000003756e6f00000000000000000000001461f08f4100000000000374776f00000003646f730000000000000000000000171ae2d95a0000000000057468726565000000047472657300000000000000000000001840689b7a000000000004666f75720000000671756174726f"));
        }

        [Fact]
        public void EncodeProduceV1()
        {
            var produceRequest = new ProduceRequest(1)
            {
                Acks = 1,
                Timeout = 60000,
                TopicData =
                {
                    new TopicProduce
                    {
                        TopicName = "Red",
                        Data =
                        {
                            new TopicPartitionProduce
                            {
                                Partition = 0,
                                MessageSet =
                                {
                                    { "one", "uno" },
                                    { "two", "dos" },
                                    { "three", "tres" },
                                    { "four", "quatro" },
                                },
                            },
                        },
                    },
                },
            };

            var stream = new MemoryStream();
            var writer = new RequestWriter(stream.AsWritable());
            writer.WriteRequest(produceRequest, 1, "test-client");
            var reqBytes = stream.ToArray();

            Assert.Equal(reqBytes, FromHex("000000b70000000100000001000b746573742d636c69656e7400010000ea600000000100035265640000000100000000000000870000000000000000000000143fe3b9f50000000000036f6e6500000003756e6f00000000000000000000001461f08f4100000000000374776f00000003646f730000000000000000000000171ae2d95a0000000000057468726565000000047472657300000000000000000000001840689b7a000000000004666f75720000000671756174726f"));
        }

        [Fact]
        public void EncodeProduceV2()
        {
            var produceRequest = new ProduceRequest(2)
            {
                Acks = 1,
                Timeout = 60000,
                TopicData =
                {
                    new TopicProduce
                    {
                        TopicName = "Red",
                        Data =
                        {
                            new TopicPartitionProduce
                            {
                                Partition = 0,
                                MessageSet =
                                {
                                    { "one", "uno" },
                                    { "two", "dos" },
                                    { "three", "tres" },
                                    { "four", "quatro" },
                                },
                            },
                        },
                    },
                },
            };

            var stream = new MemoryStream();
            var writer = new RequestWriter(stream.AsWritable());
            writer.WriteRequest(produceRequest, 1, "test-client");
            var reqBytes = stream.ToArray();

            Assert.Equal(reqBytes, FromHex("000000b70000000200000001000b746573742d636c69656e7400010000ea600000000100035265640000000100000000000000870000000000000000000000143fe3b9f50000000000036f6e6500000003756e6f00000000000000000000001461f08f4100000000000374776f00000003646f730000000000000000000000171ae2d95a0000000000057468726565000000047472657300000000000000000000001840689b7a000000000004666f75720000000671756174726f"));
        }

        [Fact]
        public void EncodeProduceBadVersion()
        {
            var version = (short)3;
            var produceRequest = new ProduceRequest(version)
            {
                Acks = 1,
                Timeout = 60000,
                TopicData =
                {
                    new TopicProduce
                    {
                        TopicName = "Red",
                        Data =
                        {
                            new TopicPartitionProduce
                            {
                                Partition = 0,
                                MessageSet =
                                {
                                    { "one", "uno" },
                                    { "two", "dos" },
                                    { "three", "tres" },
                                    { "four", "quatro" },
                                },
                            },
                        },
                    },
                },
            };

            var stream = new MemoryStream();
            var writer = new RequestWriter(stream.AsWritable());
            var ex = Assert.Throws<UnknownApiVersionException>(() =>
            {
                writer.WriteRequest(produceRequest, 1, "test-client");
            });
            Assert.Equal(ApiKey.Produce, ex.ApiKey);
            Assert.Equal(version, ex.ApiVersion);
        }

    }
}
