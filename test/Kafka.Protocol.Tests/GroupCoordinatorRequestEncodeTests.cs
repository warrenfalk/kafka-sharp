using Kafka.Protocol;
using System;
using System.IO;
using System.Linq;
using Xunit;
using static Kafka.Protocol.Tests.Helpers;

namespace Kafka.Protocol.Tests
{
    public class GroupCoordinatorRequestEncodeTests
    {
        [Fact]
        public void EncodeGroupCoordinatorV0()
        {
            var groupCoordinatorRequest = new GroupCoordinatorRequest(0)
            {
                GroupId = "test_group",
            };

            var stream = new MemoryStream();
            var writer = new RequestWriter(stream.AsWritable());
            writer.WriteRequest(groupCoordinatorRequest, 1, "test-client");
            var reqBytes = stream.ToArray();

            Assert.Equal(reqBytes, FromHex("00000021000a000000000001000b746573742d636c69656e74000a746573745f67726f7570"));
        }

        [Fact]
        public void EncodeGroupCoordinatorBadVersion()
        {
            var version = (short)1;
            var groupCoordinatorRequest = new GroupCoordinatorRequest(version)
            {
                GroupId = "test_group",
            };

            var stream = new MemoryStream();
            var writer = new RequestWriter(stream.AsWritable());
            var ex = Assert.Throws<UnknownApiVersionException>(() =>
            {
                writer.WriteRequest(groupCoordinatorRequest, 1, "test-client");
            });
            Assert.Equal(ApiKey.GroupCoordinator, ex.ApiKey);
            Assert.Equal(version, ex.ApiVersion);
        }

    }
}
