using Kafka.Protocol;
using System;
using System.IO;
using System.Linq;
using Xunit;
using static Kafka.Protocol.Tests.Helpers;

namespace Kafka.Protocol.Tests
{
    public class MetadataRequestEncode
    {
        [Fact]
        public void EncodeMetadataV0()
        {
            var metadataRequest = new MetadataRequest(0)
            {
                Topics =
                    {
                        "Red",
                        "Blue",
                    }
            };

            var stream = new MemoryStream();
            var writer = new RequestWriter(stream.AsWritable());
            writer.WriteRequest(metadataRequest, 1, "test-client");
            var reqBytes = stream.ToArray();

            Assert.Equal(reqBytes, FromHex("000000240003000000000001000b746573742d636c69656e740000000200035265640004426c7565"));
        }

        [Fact]
        public void EncodeMetadataV1()
        {
            var metadataRequest = new MetadataRequest(1)
            {
                Topics =
                    {
                        "Red",
                        "Blue",
                    }
            };

            var stream = new MemoryStream();
            var writer = new RequestWriter(stream.AsWritable());
            writer.WriteRequest(metadataRequest, 1, "test-client");
            var reqBytes = stream.ToArray();

            Assert.Equal(reqBytes, FromHex("000000240003000100000001000b746573742d636c69656e740000000200035265640004426c7565"));
        }

        [Fact]
        public void EncodeMetadataV2()
        {
            var metadataRequest = new MetadataRequest(2)
            {
                Topics =
                    {
                        "Red",
                        "Blue",
                    }
            };

            var stream = new MemoryStream();
            var writer = new RequestWriter(stream.AsWritable());
            writer.WriteRequest(metadataRequest, 1, "test-client");
            var reqBytes = stream.ToArray();

            Assert.Equal(reqBytes, FromHex("000000240003000200000001000b746573742d636c69656e740000000200035265640004426c7565"));
        }

        [Fact]
        public void EncodeMetadataBadVersion()
        {
            var version = (short)3;
            var metadataRequest = new MetadataRequest(version)
            {
                Topics =
                    {
                        "Red",
                        "Blue",
                    }
            };

            var stream = new MemoryStream();
            var writer = new RequestWriter(stream.AsWritable());
            var ex = Assert.Throws<UnknownApiVersionException>(() =>
            {
                writer.WriteRequest(metadataRequest, 1, "test-client");
            });
            Assert.Equal(ApiKey.Metadata, ex.ApiKey);
            Assert.Equal(version, ex.ApiVersion);
        }

    }
}
