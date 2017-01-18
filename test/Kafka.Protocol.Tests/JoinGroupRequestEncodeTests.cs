using Kafka.Protocol;
using System;
using System.IO;
using System.Linq;
using Xunit;
using static Kafka.Protocol.Tests.Helpers;

namespace Kafka.Protocol.Tests
{
    public class JoinGroupRequestEncodeTests
    {
        [Fact]
        public void EncodeJoinGroupV0()
        {
            var joinGroupRequest = new JoinGroupRequest(0)
            {
                GroupId = "test_group",
                SessionTimeout = 10000,
                MemberId = "member",
                ProtocolType = "proto",
                GroupProtocols =
                {
                    new GroupProtocol
                    {
                        ProtocolName = "proto1",
                        ProtocolMetadata = "proto1",
                    }
                },
            };

            var stream = new MemoryStream();
            var writer = new RequestWriter(stream.AsWritable());
            writer.WriteRequest(joinGroupRequest, 1, "test-client");
            var reqBytes = stream.ToArray();

            Assert.Equal(reqBytes, FromHex("0000004a000b000000000001000b746573742d636c69656e74000a746573745f67726f75700000271000066d656d626572000570726f746f00000001000670726f746f310000000670726f746f31"));
        }

        [Fact]
        public void EncodeJoinGroupV1()
        {
            var joinGroupRequest = new JoinGroupRequest(1)
            {
                GroupId = "test_group",
                SessionTimeout = 10000,
                RebalanceTimeout = 10000,
                MemberId = "member",
                ProtocolType = "proto",
                GroupProtocols =
                {
                    new GroupProtocol
                    {
                        ProtocolName = "proto1",
                        ProtocolMetadata = "proto1",
                    }
                },
            };

            var stream = new MemoryStream();
            var writer = new RequestWriter(stream.AsWritable());
            writer.WriteRequest(joinGroupRequest, 1, "test-client");
            var reqBytes = stream.ToArray();

            Assert.Equal(reqBytes, FromHex("0000004e000b000100000001000b746573742d636c69656e74000a746573745f67726f7570000027100000271000066d656d626572000570726f746f00000001000670726f746f310000000670726f746f31"));
        }

        [Fact]
        public void EncodeJoinGroupBadVersion()
        {
            var version = (short)2;
            var joinGroupRequest = new JoinGroupRequest(version)
            {
                GroupId = "test_group",
                SessionTimeout = 10000,
                RebalanceTimeout = 10000,
                MemberId = "member",
                ProtocolType = "proto",
                GroupProtocols =
                {
                    new GroupProtocol
                    {
                        ProtocolName = "proto1",
                        ProtocolMetadata = "proto1",
                    }
                },
            };

            var stream = new MemoryStream();
            var writer = new RequestWriter(stream.AsWritable());
            var ex = Assert.Throws<UnknownApiVersionException>(() =>
            {
                writer.WriteRequest(joinGroupRequest, 1, "test-client");
            });
            Assert.Equal(ApiKey.JoinGroup, ex.ApiKey);
            Assert.Equal(version, ex.ApiVersion);
        }

    }
}
