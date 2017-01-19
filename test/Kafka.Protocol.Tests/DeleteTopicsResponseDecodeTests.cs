using Kafka.Protocol;
using System;
using System.IO;
using System.Linq;
using Xunit;
using static Kafka.Protocol.Tests.Helpers;

namespace Kafka.Protocol.Tests
{
    public class DeleteTopicsResponseDecodeTests
    {
        [Fact]
        public void DecodeDeleteTopicsV0() 
        {
            var binary = FromHex("0000001b00000001000000020005477265656e000300064f72616e67650007");
            var pstream = new ProtocolReader(new Slice(binary));

            int size = pstream.ReadInt32();
            int correlationId = pstream.ReadInt32();

            var deleteTopicsResponse = Decode.DeleteTopicsResponse(0, pstream);
            Assert.Equal(0, deleteTopicsResponse.Version);
            var topics = deleteTopicsResponse.Topics.ToArray();
            Assert.Equal(2, topics.Length);
            {
                var topic = topics[0];
                Assert.Equal("Green", topic.TopicName);
                Assert.Equal(KafkaError.UnknownTopicOrPartition, topic.Error);
            }
            {
                var topic = topics[1];
                Assert.Equal("Orange", topic.TopicName);
                Assert.Equal(KafkaError.RequestTimedOut, topic.Error);
            }
        }

        [Fact]
        public void DecodeDeleteTopicsBadVersion()
        {
            var binary = FromHex("0000001b00000001000000020005477265656e000300064f72616e67650007");
            var pstream = new ProtocolReader(new Slice(binary));

            int size = pstream.ReadInt32();
            int correlationId = pstream.ReadInt32();


            var badVersion = (short)1;
            var ex = Assert.Throws<UnknownApiVersionException>(() =>
            {
                var deleteTopicsResponse = Decode.DeleteTopicsResponse(badVersion, pstream);
            });
            Assert.Equal(ApiKey.DeleteTopics, ex.ApiKey);
            Assert.Equal(badVersion, ex.ApiVersion);
        }

    }
}
