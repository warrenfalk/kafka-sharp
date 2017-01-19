using Kafka.Protocol;
using System;
using System.IO;
using System.Linq;
using Xunit;
using static Kafka.Protocol.Tests.Helpers;

namespace Kafka.Protocol.Tests
{
    public class CreateTopicsRequestEncodeTests
    {
        [Fact]
        public void EncodeCreateTopicsV0_NumPartitions()
        {
            var createTopicsRequest = new CreateTopicsRequest(0)
            {
                Topics =
                {
                    new CreateTopicRequest
                    {
                        TopicName = "Orange",
                        NumPartitions = 3,
                        ReplicationFactor = 1,
                        ConfigValues =
                        {
                            { "cleanup.policy", "compact" },
                            { "segment.bytes", "134217728" },
                        }
                    }
                },
                Timeout = 10000,
            };

            var stream = new MemoryStream();
            var writer = new RequestWriter(stream.AsWritable());
            writer.WriteRequest(createTopicsRequest, 1, "test-client");
            var reqBytes = stream.ToArray();

            Assert.Equal(reqBytes, FromHex("000000660013000000000001000b746573742d636c69656e740000000100064f72616e67650000000300010000000000000002000e636c65616e75702e706f6c6963790007636f6d70616374000d7365676d656e742e6279746573000931333432313737323800002710"));
        }

        [Fact]
        public void EncodeCreateTopicsV0_ReplicaAssignment()
        {
            var createTopicsRequest = new CreateTopicsRequest(0)
            {
                Topics =
                {
                    new CreateTopicRequest
                    {
                        TopicName = "Orange",
                        ReplicaAssignment =
                        {
                            { 0, 0, 1, 2 }
                        },
                        ConfigValues =
                        {
                            { "cleanup.policy", "compact" },
                            { "segment.bytes", "134217728" },
                        }
                    }
                },
                Timeout = 10000,
            };

            var stream = new MemoryStream();
            var writer = new RequestWriter(stream.AsWritable());
            writer.WriteRequest(createTopicsRequest, 1, "test-client");
            var reqBytes = stream.ToArray();

            Assert.Equal(reqBytes, FromHex("0000007a0013000000000001000b746573742d636c69656e740000000100064f72616e6765ffffffffffff00000001000000000000000300000000000000010000000200000002000e636c65616e75702e706f6c6963790007636f6d70616374000d7365676d656e742e6279746573000931333432313737323800002710"));
        }

        [Fact]
        public void EncodeCreateTopicsBadVersion()
        {
            var version = (short)1;
            var createTopicsRequest = new CreateTopicsRequest(version);

            var stream = new MemoryStream();
            var writer = new RequestWriter(stream.AsWritable());
            var ex = Assert.Throws<UnknownApiVersionException>(() =>
            {
                writer.WriteRequest(createTopicsRequest, 1, "test-client");
            });
            Assert.Equal(ApiKey.CreateTopics, ex.ApiKey);
            Assert.Equal(version, ex.ApiVersion);
        }

    }
}
