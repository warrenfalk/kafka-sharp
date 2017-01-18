using Kafka.Protocol;
using System;
using System.IO;
using System.Linq;
using Xunit;
using static Kafka.Protocol.Tests.Helpers;

namespace Kafka.Protocol.Tests
{
    public class DescribeGroupsResponseDecodeTests
    {
        [Fact]
        public void DecodeDescribeGroupsV0() 
        {
            var binary = FromHex("0000002400000001000000010000000a746573745f67726f75700004446561640000000000000000");
            var pstream = new ProtocolReader(new Slice(binary));

            int size = pstream.ReadInt32();
            int correlationId = pstream.ReadInt32();

            var describeGroupsResponse = Decode.DescribeGroupsResponse(0, pstream);
            Assert.Equal(0, describeGroupsResponse.Version);
            var groups = describeGroupsResponse.Groups.ToArray();
            Assert.Equal(1, groups.Length);
            {
                var group = groups[0];
                Assert.Equal(0, group.ErrorCode);
                Assert.Equal("test_group", group.GroupId);
                Assert.Equal("Dead", group.State);
                Assert.Equal("", group.ProtocolType);
                Assert.Equal("", group.Protocol);
                var members = group.Members.ToArray();
                Assert.Equal(0, members.Length);
            }
        }

        [Fact]
        public void DecodeDescribeGroupsBadVersion()
        {
            var binary = FromHex("0000001000000001000fffffffff0000ffffffff");
            var pstream = new ProtocolReader(new Slice(binary));

            int size = pstream.ReadInt32();
            int correlationId = pstream.ReadInt32();

            var badVersion = (short)1;
            var ex = Assert.Throws<UnknownApiVersionException>(() =>
            {
                var describeGroupsResponse = Decode.DescribeGroupsResponse(badVersion, pstream);
            });
            Assert.Equal(ApiKey.DescribeGroups, ex.ApiKey);
            Assert.Equal(badVersion, ex.ApiVersion);
        }

    }
}
