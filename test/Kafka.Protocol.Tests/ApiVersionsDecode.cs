using Kafka.Protocol;
using System;
using System.IO;
using System.Linq;
using Xunit;
using static Kafka.Protocol.Tests.Helpers;

namespace Kafka.Protocol.Tests
{
    public class ApiVersionsDecode
    {
        [Fact]
        public void DecodeApiVersionsV0() 
        {
            var binary = FromHex("0000008800000001000000000015000000000002000100000003000200000001000300000002000400000000000500000000000600000002000700010001000800000002000900000001000a00000000000b00000001000c00000000000d00000000000e00000000000f00000000001000000000001100000000001200000000001300000000001400000000");
            var pstream = new ProtocolReader(new Slice(binary));

            int size = pstream.ReadInt32();
            int correlationId = pstream.ReadInt32();

            var apiVersionsResponse = Decode.ApiVersionsResponse(0, pstream);
            Assert.Equal(0, apiVersionsResponse.Version);
            Assert.Equal(0, apiVersionsResponse.ErrorCode);
            var versions = apiVersionsResponse.ApiVersions.ToArray();
            Assert.Equal(21, versions.Length);
            ApiVersionSupport avs;
            avs = versions[0];
            Assert.Equal((short)ApiKey.Produce, avs.ApiKeyCode);
            Assert.Equal(0, avs.MinVersion);
            Assert.Equal(2, avs.MaxVersion);
            avs = versions[1];
            Assert.Equal((short)ApiKey.Fetch, avs.ApiKeyCode);
            Assert.Equal(0, avs.MinVersion);
            Assert.Equal(3, avs.MaxVersion);
            avs = versions[2];
            Assert.Equal((short)ApiKey.Offsets, avs.ApiKeyCode);
            Assert.Equal(0, avs.MinVersion);
            Assert.Equal(1, avs.MaxVersion);
            avs = versions[3];
            Assert.Equal((short)ApiKey.Metadata, avs.ApiKeyCode);
            Assert.Equal(0, avs.MinVersion);
            Assert.Equal(2, avs.MaxVersion);
            avs = versions[4];
            Assert.Equal((short)ApiKey.LeaderAndIsr, avs.ApiKeyCode);
            Assert.Equal(0, avs.MinVersion);
            Assert.Equal(0, avs.MaxVersion);
            avs = versions[5];
            Assert.Equal((short)ApiKey.StopReplica, avs.ApiKeyCode);
            Assert.Equal(0, avs.MinVersion);
            Assert.Equal(0, avs.MaxVersion);
            avs = versions[6];
            Assert.Equal((short)ApiKey.UpdateMetadata, avs.ApiKeyCode);
            Assert.Equal(0, avs.MinVersion);
            Assert.Equal(2, avs.MaxVersion);
            avs = versions[7];
            Assert.Equal((short)ApiKey.ControlledShutdown, avs.ApiKeyCode);
            Assert.Equal(1, avs.MinVersion);
            Assert.Equal(1, avs.MaxVersion);
            avs = versions[8];
            Assert.Equal((short)ApiKey.OffsetCommit, avs.ApiKeyCode);
            Assert.Equal(0, avs.MinVersion);
            Assert.Equal(2, avs.MaxVersion);
            avs = versions[9];
            Assert.Equal((short)ApiKey.OffsetFetch, avs.ApiKeyCode);
            Assert.Equal(0, avs.MinVersion);
            Assert.Equal(1, avs.MaxVersion);
            avs = versions[10];
            Assert.Equal((short)ApiKey.GroupCoordinator, avs.ApiKeyCode);
            Assert.Equal(0, avs.MinVersion);
            Assert.Equal(0, avs.MaxVersion);
            avs = versions[11];
            Assert.Equal((short)ApiKey.JoinGroup, avs.ApiKeyCode);
            Assert.Equal(0, avs.MinVersion);
            Assert.Equal(1, avs.MaxVersion);
            avs = versions[12];
            Assert.Equal((short)ApiKey.Heartbeat, avs.ApiKeyCode);
            Assert.Equal(0, avs.MinVersion);
            Assert.Equal(0, avs.MaxVersion);
            avs = versions[13];
            Assert.Equal((short)ApiKey.LeaveGroup, avs.ApiKeyCode);
            Assert.Equal(0, avs.MinVersion);
            Assert.Equal(0, avs.MaxVersion);
            avs = versions[14];
            Assert.Equal((short)ApiKey.SyncGroup, avs.ApiKeyCode);
            Assert.Equal(0, avs.MinVersion);
            Assert.Equal(0, avs.MaxVersion);
            avs = versions[15];
            Assert.Equal((short)ApiKey.DescribeGroups, avs.ApiKeyCode);
            Assert.Equal(0, avs.MinVersion);
            Assert.Equal(0, avs.MaxVersion);
            avs = versions[16];
            Assert.Equal((short)ApiKey.ListGroups, avs.ApiKeyCode);
            Assert.Equal(0, avs.MinVersion);
            Assert.Equal(0, avs.MaxVersion);
            avs = versions[17];
            Assert.Equal((short)ApiKey.SaslHandshake, avs.ApiKeyCode);
            Assert.Equal(0, avs.MinVersion);
            Assert.Equal(0, avs.MaxVersion);
            avs = versions[18];
            Assert.Equal((short)ApiKey.ApiVersions, avs.ApiKeyCode);
            Assert.Equal(0, avs.MinVersion);
            Assert.Equal(0, avs.MaxVersion);
            avs = versions[19];
            Assert.Equal((short)ApiKey.CreateTopics, avs.ApiKeyCode);
            Assert.Equal(0, avs.MinVersion);
            Assert.Equal(0, avs.MaxVersion);
            avs = versions[20];
            Assert.Equal((short)ApiKey.DeleteTopics, avs.ApiKeyCode);
            Assert.Equal(0, avs.MinVersion);
            Assert.Equal(0, avs.MaxVersion);
        }

        [Fact]
        public void DecodeApiVersionsBadVersion()
        {
            var binary = FromHex("0000008800000001000000000015000000000002000100000003000200000001000300000002000400000000000500000000000600000002000700010001000800000002000900000001000a00000000000b00000001000c00000000000d00000000000e00000000000f00000000001000000000001100000000001200000000001300000000001400000000");
            var pstream = new ProtocolReader(new Slice(binary));

            int size = pstream.ReadInt32();
            int correlationId = pstream.ReadInt32();


            var badVersion = (short)1;
            var ex = Assert.Throws<UnknownApiVersionException>(() =>
            {
                var apiVersionsResponse = Decode.ApiVersionsResponse(badVersion, pstream);
            });
            Assert.Equal(ApiKey.ApiVersions, ex.ApiKey);
            Assert.Equal(badVersion, ex.ApiVersion);
        }

    }
}
