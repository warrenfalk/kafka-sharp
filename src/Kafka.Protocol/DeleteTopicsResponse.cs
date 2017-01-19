using System;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Protocol
{
    public interface DeleteTopicsResponse
    {
        int Version { get; }
        IEnumerable<DeleteTopicResponse> Topics { get; }
    }

    public interface DeleteTopicResponse
    {
        string TopicName { get; }
        short ErrorCode { get; }
    }

    class DeleteTopicsResponseImpl : DeleteTopicsResponse
    {
        public int Version { get; }
        public IEnumerable<DeleteTopicResponse> Topics { get; }

        public DeleteTopicsResponseImpl(
            int version,
            IEnumerable<DeleteTopicResponse> topics)
        {
            Version = version;
            Topics = topics;
        }

        public static DecoderVersions<DeleteTopicsResponse> Versions = new DecoderVersions<DeleteTopicsResponse>(
            ApiKey.DeleteTopics,
            reader => new DeleteTopicsResponseImpl(
                version: 0,
                topics: reader.ReadList(DeleteTopicResponseImpl.Versions[0])
            )
        );
    }

    class DeleteTopicResponseImpl : DeleteTopicResponse
    {
        public string TopicName { get; }
        public short ErrorCode { get; }
        public DeleteTopicResponseImpl(
            string topicName,
            short errorCode)
        {
            TopicName = topicName;
            ErrorCode = errorCode;
        }

        public static DecoderVersions<DeleteTopicResponse> Versions = new DecoderVersions<DeleteTopicResponse>(
            ApiKey.None,
            reader => new DeleteTopicResponseImpl(
                topicName: reader.ReadString(),
                errorCode: reader.ReadInt16()
            )
        );
    }

}
