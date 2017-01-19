using System;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Protocol
{
    public interface CreateTopicsResponse
    {
        int Version { get; }
        IEnumerable<CreateTopicResponse> Topics { get; }
    }

    public interface CreateTopicResponse
    {
        string TopicName { get; }
        KafkaError Error { get; }
    }

    class CreateTopicsResponseImpl : CreateTopicsResponse
    {
        public int Version { get; }
        public IEnumerable<CreateTopicResponse> Topics { get; }

        public CreateTopicsResponseImpl(
            int version,
            IEnumerable<CreateTopicResponse> topics)
        {
            Version = version;
            Topics = topics;
        }

        public static DecoderVersions<CreateTopicsResponse> Versions = new DecoderVersions<CreateTopicsResponse>(
            ApiKey.CreateTopics,
            reader => new CreateTopicsResponseImpl(
                version: 0,
                topics: reader.ReadList(CreateTopicResponseImpl.Versions[0])
            )
        );
    }

    class CreateTopicResponseImpl : CreateTopicResponse
    {
        public string TopicName { get; }
        public KafkaError Error { get; }
        public CreateTopicResponseImpl(
            string topicName,
            KafkaError error)
        {
            TopicName = topicName;
            Error = error;
        }

        public static DecoderVersions<CreateTopicResponse> Versions = new DecoderVersions<CreateTopicResponse>(
            ApiKey.None,
            reader => new CreateTopicResponseImpl(
                topicName: reader.ReadString(),
                error: reader.ReadErrorCode()
            )
        );
    }

}
