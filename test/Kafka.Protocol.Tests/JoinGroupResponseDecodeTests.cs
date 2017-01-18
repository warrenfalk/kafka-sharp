using Kafka.Protocol;
using System;
using System.IO;
using System.Linq;
using Xunit;
using static Kafka.Protocol.Tests.Helpers;

namespace Kafka.Protocol.Tests
{
    public class JoinGroupResponseDecodeTests
    {
        [Fact]
        public void DecodeJoinGroupV0() 
        {
            var binary = FromHex("0000001a000000010019000000000000000000066d656d62657200000000");
            var pstream = new ProtocolReader(new Slice(binary));

            int size = pstream.ReadInt32();
            int correlationId = pstream.ReadInt32();

            var joinGroupResponse = Decode.JoinGroupResponse(0, pstream);
            Assert.Equal(0, joinGroupResponse.Version);
            Assert.Equal(25, joinGroupResponse.ErrorCode);
            Assert.Equal(0, joinGroupResponse.GenerationId);
            Assert.Equal("", joinGroupResponse.GroupProtocol);
            Assert.Equal("", joinGroupResponse.LeaderId);
            Assert.Equal("member", joinGroupResponse.MemberId);
            var members = joinGroupResponse.Members.ToArray();
            Assert.Equal(0, members.Length);
        }

        [Fact]
        public void DecodeJoinGroupV1()
        {
            var binary = FromHex("0000001a000000010019000000000000000000066d656d62657200000000");
            var pstream = new ProtocolReader(new Slice(binary));

            int size = pstream.ReadInt32();
            int correlationId = pstream.ReadInt32();

            var joinGroupResponse = Decode.JoinGroupResponse(1, pstream);
            Assert.Equal(1, joinGroupResponse.Version);
            Assert.Equal(25, joinGroupResponse.ErrorCode);
            Assert.Equal(0, joinGroupResponse.GenerationId);
            Assert.Equal("", joinGroupResponse.GroupProtocol);
            Assert.Equal("", joinGroupResponse.LeaderId);
            Assert.Equal("member", joinGroupResponse.MemberId);
            var members = joinGroupResponse.Members.ToArray();
            Assert.Equal(0, members.Length);
        }

        [Fact]
        public void DecodeJoinGroupBadVersion()
        {
            var binary = FromHex("0000002b00000001000000010003526564000000010000000000000000000000000008ffffffffffffffff00000000");
            var pstream = new ProtocolReader(new Slice(binary));

            int size = pstream.ReadInt32();
            int correlationId = pstream.ReadInt32();

            var badVersion = (short)2;
            var ex = Assert.Throws<UnknownApiVersionException>(() =>
            {
                var joinGroupResponse = Decode.JoinGroupResponse(badVersion, pstream);
            });
            Assert.Equal(ApiKey.JoinGroup, ex.ApiKey);
            Assert.Equal(badVersion, ex.ApiVersion);
        }

    }
}
