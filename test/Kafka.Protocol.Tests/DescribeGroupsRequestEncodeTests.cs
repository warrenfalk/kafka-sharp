using Kafka.Protocol;
using System;
using System.IO;
using System.Linq;
using Xunit;
using static Kafka.Protocol.Tests.Helpers;

namespace Kafka.Protocol.Tests
{
    public class DescribeGroupsRequestEncodeTests
    {
        [Fact]
        public void EncodeDescribeGroupsV0()
        {
            var describeGroupsRequest = new DescribeGroupsRequest(0)
            {
                GroupIds =
                {
                    "test_group",
                }
            };

            var stream = new MemoryStream();
            var writer = new RequestWriter(stream.AsWritable());
            writer.WriteRequest(describeGroupsRequest, 1, "test-client");
            var reqBytes = stream.ToArray();

            Assert.Equal(reqBytes, FromHex("00000025000f000000000001000b746573742d636c69656e7400000001000a746573745f67726f7570"));
        }


        [Fact]
        public void EncodeDescribeGroupsBadVersion()
        {
            var version = (short)1;
            var describeGroupsRequest = new DescribeGroupsRequest(version)
            {
                GroupIds =
                {
                    "test_group",
                }
            };

            var stream = new MemoryStream();
            var writer = new RequestWriter(stream.AsWritable());
            var ex = Assert.Throws<UnknownApiVersionException>(() =>
            {
                writer.WriteRequest(describeGroupsRequest, 1, "test-client");
            });
            Assert.Equal(ApiKey.DescribeGroups, ex.ApiKey);
            Assert.Equal(version, ex.ApiVersion);
        }

    }
}
