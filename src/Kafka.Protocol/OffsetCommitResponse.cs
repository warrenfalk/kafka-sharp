using System;
using System.Collections.Generic;

namespace Kafka.Protocol
{
    public interface OffsetCommitResponse
    {
        int Version { get; }
        IEnumerable<TopicOffsetCommitResponse> Topics { get; }
    }

    public interface TopicOffsetCommitResponse
    {
        string TopicName { get; }
        IEnumerable<PartitionOffsetCommitResponse> Partitions { get; }
    }

    public interface PartitionOffsetCommitResponse
    {
        int Partition { get; }
        KafkaError Error { get; }
    }

    class OffsetCommitResponseImpl : OffsetCommitResponse
    {
        public int Version { get; }
        public IEnumerable<TopicOffsetCommitResponse> Topics { get; }

        public OffsetCommitResponseImpl(
            int version,
            IEnumerable<TopicOffsetCommitResponse> topics)
        {
            Version = version;
            Topics = topics;
        }

        public static DecoderVersions<OffsetCommitResponse> Decode = new DecoderVersions<OffsetCommitResponse>(
            ApiKey.OffsetCommit,
            reader => new OffsetCommitResponseImpl(
                version: 0,
                topics: reader.ReadList(TopicOffsetCommitResponseImpl.Versions[0])
            ),
            reader => new OffsetCommitResponseImpl(
                version: 1,
                topics: reader.ReadList(TopicOffsetCommitResponseImpl.Versions[0])
            ),
            reader => new OffsetCommitResponseImpl(
                version: 2,
                topics: reader.ReadList(TopicOffsetCommitResponseImpl.Versions[0])
            )
        );
    }

    class TopicOffsetCommitResponseImpl : TopicOffsetCommitResponse
    {
        public string TopicName { get; }
        public IEnumerable<PartitionOffsetCommitResponse> Partitions { get; }

        public TopicOffsetCommitResponseImpl(
            string topicName,
            IEnumerable<PartitionOffsetCommitResponse> partitions)
        {
            TopicName = topicName;
            Partitions = partitions;
        }

        public static DecoderVersions<TopicOffsetCommitResponse> Versions = new DecoderVersions<TopicOffsetCommitResponse>(
            ApiKey.None,
            reader => new TopicOffsetCommitResponseImpl(
                topicName: reader.ReadString(),
                partitions: reader.ReadList(PartitionOffsetCommitResponseImpl.Versions[0])
            )
        );
    }

    class PartitionOffsetCommitResponseImpl : PartitionOffsetCommitResponse
    {
        public int Partition { get; }
        public KafkaError Error { get; }

        public PartitionOffsetCommitResponseImpl(
            int partition,
            KafkaError error)
        {
            Partition = partition;
            Error = error;
        }

        public static DecoderVersions<PartitionOffsetCommitResponse> Versions = new DecoderVersions<PartitionOffsetCommitResponse>(
            ApiKey.None,
            reader => new PartitionOffsetCommitResponseImpl(
                partition: reader.ReadInt32(),
                error: reader.ReadErrorCode()
            )
        );
    }
}
