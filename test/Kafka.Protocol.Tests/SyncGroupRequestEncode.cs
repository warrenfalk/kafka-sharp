using Kafka.Protocol;
using System;
using System.IO;
using System.Linq;
using Xunit;
using static Kafka.Protocol.Tests.Helpers;

namespace Kafka.Protocol.Tests
{
    public class SyncGroupRequestEncode
    {
        [Fact]
        public void EncodeSyncGroupV0()
        {
            var syncGroupRequest = new SyncGroupRequest(0)
            {
                GroupId = "test_group",
                GenerationId = 1,
                MemberId = "member",
                GroupAssignments =
                {
                    new GroupAssignment
                    {
                        MemberId = "member",
                        MemberAssignment = "assignment",
                    }
                },
            };

            var stream = new MemoryStream();
            var writer = new RequestWriter(stream.AsWritable());
            writer.WriteRequest(syncGroupRequest, 1, "test-client");
            var reqBytes = stream.ToArray();

            Assert.Equal(reqBytes, FromHex("00000047000e000000000001000b746573742d636c69656e74000a746573745f67726f75700000000100066d656d6265720000000100066d656d6265720000000a61737369676e6d656e74"));
        }

        [Fact]
        public void EncodeSyncGroupBadVersion()
        {
            var version = (short)1;
            var syncGroupRequest = new SyncGroupRequest(version)
            {
                GroupId = "test_group",
                GenerationId = 1,
                MemberId = "member",
                GroupAssignments =
                {
                    new GroupAssignment
                    {
                        MemberId = "member",
                        MemberAssignment = "assignment",
                    }
                },
            };

            var stream = new MemoryStream();
            var writer = new RequestWriter(stream.AsWritable());
            var ex = Assert.Throws<UnknownApiVersionException>(() =>
            {
                writer.WriteRequest(syncGroupRequest, 1, "test-client");
            });
            Assert.Equal(ApiKey.SyncGroup, ex.ApiKey);
            Assert.Equal(version, ex.ApiVersion);
        }

    }
}
