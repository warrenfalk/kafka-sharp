using Kafka.Protocol;
using System;
using System.IO;
using System.Linq;
using Xunit;
using static Kafka.Protocol.Tests.Helpers;

namespace Kafka.Protocol.Tests
{
    public class ListGroupsRequestEncodeTests
    {
        [Fact]
        public void EncodeListGroupsV0()
        {
            var listGroupsRequest = new ListGroupsRequest(0)
            {
            };

            var stream = new MemoryStream();
            var writer = new RequestWriter(stream.AsWritable());
            writer.WriteRequest(listGroupsRequest, 1, "test-client");
            var reqBytes = stream.ToArray();

            Assert.Equal(reqBytes, FromHex("000000150010000000000001000b746573742d636c69656e74"));
        }

        [Fact]
        public void EncodeListGroupsBadVersion()
        {
            var version = (short)1;
            var listGroupsRequest = new ListGroupsRequest(version)
            {
            };

            var stream = new MemoryStream();
            var writer = new RequestWriter(stream.AsWritable());
            var ex = Assert.Throws<UnknownApiVersionException>(() =>
            {
                writer.WriteRequest(listGroupsRequest, 1, "test-client");
            });
            Assert.Equal(ApiKey.ListGroups, ex.ApiKey);
            Assert.Equal(version, ex.ApiVersion);
        }

    }
}
