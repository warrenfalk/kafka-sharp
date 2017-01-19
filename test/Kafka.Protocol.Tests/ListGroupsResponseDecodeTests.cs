using Kafka.Protocol;
using System;
using System.IO;
using System.Linq;
using Xunit;
using static Kafka.Protocol.Tests.Helpers;

namespace Kafka.Protocol.Tests
{
    public class ListGroupsResponseDecodeTests
    {
        [Fact]
        public void DecodeListGroupsV0() 
        {
            var binary = FromHex("0000000a00000001000000000000");
            var pstream = new ProtocolReader(new Slice(binary));

            int size = pstream.ReadInt32();
            int correlationId = pstream.ReadInt32();

            var listGroupsResponse = Decode.ListGroupsResponse(0, pstream);
            Assert.Equal(0, listGroupsResponse.Version);
            Assert.Equal(KafkaError.None, listGroupsResponse.Error);
            var groups = listGroupsResponse.Groups.ToArray();
            // TODO: get test case that actually has data
            Assert.Equal(0, groups.Length);
        }

        [Fact]
        public void DecodeListGroupsBadVersion()
        {
            var binary = FromHex("0000000a00000001000000000000");
            var pstream = new ProtocolReader(new Slice(binary));

            int size = pstream.ReadInt32();
            int correlationId = pstream.ReadInt32();

            var badVersion = (short)1;
            var ex = Assert.Throws<UnknownApiVersionException>(() =>
            {
                var listGroupsResponse = Decode.ListGroupsResponse(badVersion, pstream);
            });
            Assert.Equal(ApiKey.ListGroups, ex.ApiKey);
            Assert.Equal(badVersion, ex.ApiVersion);
        }

    }
}
