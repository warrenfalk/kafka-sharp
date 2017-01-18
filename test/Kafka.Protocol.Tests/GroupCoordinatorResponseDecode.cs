using Kafka.Protocol;
using System;
using System.IO;
using System.Linq;
using Xunit;
using static Kafka.Protocol.Tests.Helpers;

namespace Kafka.Protocol.Tests
{
    public class GroupCoordinatorResponseDecode
    {
        [Fact]
        public void DecodeGroupCoordinatorV0() 
        {
            var binary = FromHex("0000001000000001000fffffffff0000ffffffff");
            var pstream = new ProtocolReader(new Slice(binary));

            int size = pstream.ReadInt32();
            int correlationId = pstream.ReadInt32();

            var groupCoordinatorResponse = Decode.GroupCoordinatorResponse(0, pstream);
            Assert.Equal(0, groupCoordinatorResponse.Version);
            Assert.Equal(15, groupCoordinatorResponse.ErrorCode);
            Assert.Equal(-1, groupCoordinatorResponse.Coordinator.NodeId);
            Assert.Equal("", groupCoordinatorResponse.Coordinator.Host);
            Assert.Equal(-1, groupCoordinatorResponse.Coordinator.Port);
        }

        [Fact]
        public void DecodeGroupCoordinatorBadVersion()
        {
            var binary = FromHex("0000001000000001000fffffffff0000ffffffff");
            var pstream = new ProtocolReader(new Slice(binary));

            int size = pstream.ReadInt32();
            int correlationId = pstream.ReadInt32();

            var badVersion = (short)1;
            var ex = Assert.Throws<UnknownApiVersionException>(() =>
            {
                var groupCoordinatorResponse = Decode.GroupCoordinatorResponse(badVersion, pstream);
            });
            Assert.Equal(ApiKey.GroupCoordinator, ex.ApiKey);
            Assert.Equal(badVersion, ex.ApiVersion);
        }

    }
}
