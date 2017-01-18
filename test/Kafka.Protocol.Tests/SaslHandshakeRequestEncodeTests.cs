using Kafka.Protocol;
using System;
using System.IO;
using System.Linq;
using Xunit;
using static Kafka.Protocol.Tests.Helpers;

namespace Kafka.Protocol.Tests
{
    public class SaslHandshakeRequestEncodeTests
    {
        [Fact]
        public void EncodeSaslHandshakeV0()
        {
            var saslHandshakeRequest = new SaslHandshakeRequest(0)
            {
                Mechanism = "fake"
            };

            var stream = new MemoryStream();
            var writer = new RequestWriter(stream.AsWritable());
            writer.WriteRequest(saslHandshakeRequest, 1, "test-client");
            var reqBytes = stream.ToArray();

            Assert.Equal(reqBytes, FromHex("0000001b0011000000000001000b746573742d636c69656e74000466616b65"));
        }

        [Fact]
        public void EncodeSaslHandshakeBadVersion()
        {
            var version = (short)1;
            var saslHandshakeRequest = new SaslHandshakeRequest(version)
            {
                Mechanism = "fake"
            };

            var stream = new MemoryStream();
            var writer = new RequestWriter(stream.AsWritable());
            var ex = Assert.Throws<UnknownApiVersionException>(() =>
            {
                writer.WriteRequest(saslHandshakeRequest, 1, "test-client");
            });
            Assert.Equal(ApiKey.SaslHandshake, ex.ApiKey);
            Assert.Equal(version, ex.ApiVersion);
        }

    }
}
