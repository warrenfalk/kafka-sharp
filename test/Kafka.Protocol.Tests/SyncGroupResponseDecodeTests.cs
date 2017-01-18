using Kafka.Protocol;
using System;
using System.IO;
using System.Linq;
using Xunit;
using static Kafka.Protocol.Tests.Helpers;

namespace Kafka.Protocol.Tests
{
    public class SyncGroupResponseDecodeTests
    {
        [Fact]
        public void DecodeSyncGroupV0() 
        {
            var binary = FromHex("0000000a00000001001900000000");
            var pstream = new ProtocolReader(new Slice(binary));

            int size = pstream.ReadInt32();
            int correlationId = pstream.ReadInt32();

            var syncGroupResponse = Decode.SyncGroupResponse(0, pstream);
            Assert.Equal(0, syncGroupResponse.Version);
            Assert.Equal(25, syncGroupResponse.ErrorCode);
            Assert.True(syncGroupResponse.MemberAssignment.Length == 0);
        }

        [Fact]
        public void DecodeSyncGroupBadVersion()
        {
            var binary = FromHex("0000000a00000001001900000000");
            var pstream = new ProtocolReader(new Slice(binary));

            int size = pstream.ReadInt32();
            int correlationId = pstream.ReadInt32();

            var badVersion = (short)1;
            var ex = Assert.Throws<UnknownApiVersionException>(() =>
            {
                var syncGroupResponse = Decode.SyncGroupResponse(badVersion, pstream);
            });
            Assert.Equal(ApiKey.SyncGroup, ex.ApiKey);
            Assert.Equal(badVersion, ex.ApiVersion);
        }

    }
}
