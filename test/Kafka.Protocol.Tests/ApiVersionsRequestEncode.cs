using Kafka.Protocol;
using System;
using System.IO;
using System.Linq;
using Xunit;
using static Kafka.Protocol.Tests.Helpers;

namespace Kafka.Protocol.Tests
{
    public class ApiVersionsRequestEncode
    {
        [Fact]
        public void EncodeApiVersionsV0()
        {
            var apiVersionsRequest = new ApiVersionsRequest(0)
            {
            };

            var stream = new MemoryStream();
            var writer = new RequestWriter(stream.AsWritable());
            writer.WriteRequest(apiVersionsRequest, 1, "test-client");
            var reqBytes = stream.ToArray();

            Assert.Equal(reqBytes, FromHex("000000150012000000000001000b746573742d636c69656e74"));
        }

        [Fact]
        public void EncodeApiVersionsBadVersion()
        {
            var version = (short)1;
            var apiVersionsRequest = new ApiVersionsRequest(version)
            {
            };

            var stream = new MemoryStream();
            var writer = new RequestWriter(stream.AsWritable());
            var ex = Assert.Throws<UnknownApiVersionException>(() =>
            {
                writer.WriteRequest(apiVersionsRequest, 1, "test-client");
            });
            Assert.Equal(ApiKey.ApiVersions, ex.ApiKey);
            Assert.Equal(version, ex.ApiVersion);
        }

    }
}
