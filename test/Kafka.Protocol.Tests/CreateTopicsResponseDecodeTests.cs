using Kafka.Protocol;
using System;
using System.IO;
using System.Linq;
using Xunit;
using static Kafka.Protocol.Tests.Helpers;

namespace Kafka.Protocol.Tests
{
    public class CreateTopicsResponseDecodeTests
    {
        [Fact]
        public void DecodeCreateTopicsV0() 
        {
            var binary = FromHex("00000012000000010000000100064f72616e67650000");
            var pstream = new ProtocolReader(new Slice(binary));

            int size = pstream.ReadInt32();
            int correlationId = pstream.ReadInt32();

            var createTopicsResponse = Decode.CreateTopicsResponse(0, pstream);
            Assert.Equal(0, createTopicsResponse.Version);
            var topics = createTopicsResponse.Topics.ToArray();
            Assert.Equal(1, topics.Length);
            {
                var topic = topics[0];
                Assert.Equal("Orange", topic.TopicName);
                Assert.Equal(KafkaError.None, topic.Error);
            }
        }

        [Fact]
        public void DecodeCreateTopicsBadVersion()
        {
            var binary = FromHex("00000012000000010022000000010006475353415049");
            var pstream = new ProtocolReader(new Slice(binary));

            int size = pstream.ReadInt32();
            int correlationId = pstream.ReadInt32();


            var badVersion = (short)1;
            var ex = Assert.Throws<UnknownApiVersionException>(() =>
            {
                var createTopicsResponse = Decode.CreateTopicsResponse(badVersion, pstream);
            });
            Assert.Equal(ApiKey.CreateTopics, ex.ApiKey);
            Assert.Equal(badVersion, ex.ApiVersion);
        }

    }
}
