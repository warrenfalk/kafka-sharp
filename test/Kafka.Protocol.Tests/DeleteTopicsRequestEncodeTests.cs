using Kafka.Protocol;
using System;
using System.IO;
using System.Linq;
using Xunit;
using static Kafka.Protocol.Tests.Helpers;

namespace Kafka.Protocol.Tests
{
    public class DeleteTopicsRequestEncodeTests
    {
        [Fact]
        public void EncodeDeleteTopicsV0_NumPartitions()
        {
            var deleteTopicsRequest = new DeleteTopicsRequest(0)
            {
                Topics = { "Orange", "Green" },
                Timeout = 10000,
            };

            var stream = new MemoryStream();
            var writer = new RequestWriter(stream.AsWritable());
            writer.WriteRequest(deleteTopicsRequest, 1, "test-client");
            var reqBytes = stream.ToArray();

            Assert.Equal(reqBytes, FromHex("0000002c0014000000000001000b746573742d636c69656e740000000200064f72616e67650005477265656e00002710"));
        }

        [Fact]
        public void EncodeDeleteTopicsBadVersion()
        {
            var version = (short)1;
            var deleteTopicsRequest = new DeleteTopicsRequest(version);

            var stream = new MemoryStream();
            var writer = new RequestWriter(stream.AsWritable());
            var ex = Assert.Throws<UnknownApiVersionException>(() =>
            {
                writer.WriteRequest(deleteTopicsRequest, 1, "test-client");
            });
            Assert.Equal(ApiKey.DeleteTopics, ex.ApiKey);
            Assert.Equal(version, ex.ApiVersion);
        }

    }
}
