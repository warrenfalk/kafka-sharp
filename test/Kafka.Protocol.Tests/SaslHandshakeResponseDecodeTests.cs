using Kafka.Protocol;
using System;
using System.IO;
using System.Linq;
using Xunit;
using static Kafka.Protocol.Tests.Helpers;

namespace Kafka.Protocol.Tests
{
    public class SaslHandshakeResponseDecodeTests
    {
        [Fact]
        public void DecodeSaslHandshakeV0() 
        {
            var binary = FromHex("00000012000000010022000000010006475353415049");
            var pstream = new ProtocolReader(new Slice(binary));

            int size = pstream.ReadInt32();
            int correlationId = pstream.ReadInt32();

            var saslHandshakeResponse = Decode.SaslHandshakeResponse(0, pstream);
            Assert.Equal(0, saslHandshakeResponse.Version);
            Assert.Equal(34, saslHandshakeResponse.ErrorCode);
            Assert.Equal(new string[] { "GSSAPI" }, saslHandshakeResponse.EnabledMechanisms);
        }

        [Fact]
        public void DecodeSaslHandshakeBadVersion()
        {
            var binary = FromHex("00000012000000010022000000010006475353415049");
            var pstream = new ProtocolReader(new Slice(binary));

            int size = pstream.ReadInt32();
            int correlationId = pstream.ReadInt32();


            var badVersion = (short)1;
            var ex = Assert.Throws<UnknownApiVersionException>(() =>
            {
                var saslHandshakeResponse = Decode.SaslHandshakeResponse(badVersion, pstream);
            });
            Assert.Equal(ApiKey.SaslHandshake, ex.ApiKey);
            Assert.Equal(badVersion, ex.ApiVersion);
        }

    }
}
