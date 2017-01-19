using System;
using System.Collections.Generic;

namespace Kafka.Protocol
{
    public interface ProduceResponse
    {
        int Version { get; }
        IEnumerable<TopicProduceResponse> Topics { get; }
        int ThrottleTimeMs { get; }
    }

    public interface TopicProduceResponse
    {
        string TopicName { get; }
        IEnumerable<PartitionProduceResponse> Partitions { get; }
    }

    public interface PartitionProduceResponse
    {
        int Partition { get; }
        KafkaError Error { get; }
        long BaseOffset { get; }
        long Timestamp { get; }
    }

    class ProduceResponseImpl : ProduceResponse
    {
        public int Version { get; }
        public IEnumerable<TopicProduceResponse> Topics { get; }
        public int ThrottleTimeMs { get; }

        public ProduceResponseImpl(
            int version,
            IEnumerable<TopicProduceResponse> topics,
            int throttleTimeMs = -1)
        {
            Version = version;
            Topics = topics;
            ThrottleTimeMs = throttleTimeMs;
        }

        public static DecoderVersions<ProduceResponse> Decode = new DecoderVersions<ProduceResponse>(
            ApiKey.Produce,
            reader => new ProduceResponseImpl(
                version: 0,
                topics: reader.ReadList(TopicProduceResponseImpl.Versions[0])
            ),
            reader => new ProduceResponseImpl(
                version: 1,
                topics: reader.ReadList(TopicProduceResponseImpl.Versions[0]),
                throttleTimeMs: reader.ReadInt32()
            ),
            reader => new ProduceResponseImpl(
                version: 2,
                topics: reader.ReadList(TopicProduceResponseImpl.Versions[1]),
                throttleTimeMs: reader.ReadInt32()
            )
        );
    }

    class TopicProduceResponseImpl : TopicProduceResponse
    {
        public string TopicName { get; }
        public IEnumerable<PartitionProduceResponse> Partitions { get; }

        public TopicProduceResponseImpl(
            string topicName,
            IEnumerable<PartitionProduceResponse> partitions)
        {
            TopicName = topicName;
            Partitions = partitions;
        }

        public static DecoderVersions<TopicProduceResponse> Versions = new DecoderVersions<TopicProduceResponse>(
            ApiKey.None,
            reader => new TopicProduceResponseImpl(
                topicName: reader.ReadString(),
                partitions: reader.ReadList(PartitionProduceResponseImpl.Versions[0])
            ),
            reader => new TopicProduceResponseImpl(
                topicName: reader.ReadString(),
                partitions: reader.ReadList(PartitionProduceResponseImpl.Versions[1])
            )
        );
    }

    class PartitionProduceResponseImpl : PartitionProduceResponse
    {
        public int Partition { get; }
        public KafkaError Error { get; }
        public long BaseOffset { get; }
        public long Timestamp { get; }

        public PartitionProduceResponseImpl(
            int partition,
            KafkaError error,
            long baseOffset,
            long timestamp = -1)
        {
            Partition = partition;
            Error = error;
            BaseOffset = baseOffset;
            Timestamp = timestamp;
        }

        public static DecoderVersions<PartitionProduceResponse> Versions = new DecoderVersions<PartitionProduceResponse>(
            ApiKey.None,
            reader => new PartitionProduceResponseImpl(
                partition: reader.ReadInt32(),
                error: reader.ReadErrorCode(),
                baseOffset: reader.ReadInt64()
            ),
            reader => new PartitionProduceResponseImpl(
                partition: reader.ReadInt32(),
                error: reader.ReadErrorCode(),
                baseOffset: reader.ReadInt64(),
                timestamp: reader.ReadInt64()
            )
        );
    }
}
