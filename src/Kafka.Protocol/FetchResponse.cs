using System;
using System.Collections.Generic;

namespace Kafka.Protocol
{
    public interface FetchResponse
    {
        int Version { get; }
        int ThrottleTimeMs { get; } // Version 1+
        IEnumerable<TopicFetchResponse> Topics { get; }
    }

    public interface TopicFetchResponse
    {
        string TopicName { get; }
        IEnumerable<PartitionFetchResponse> Partitions { get; }
    }

    public interface PartitionFetchResponse
    {
        int Partition { get; }
        KafkaError Error { get; }
        long HighWatermark { get; }
        IEnumerable<Message> MessageSet { get; }
    }

    class FetchResponseImpl : FetchResponse
    {
        public int Version { get; }
        public IEnumerable<TopicFetchResponse> Topics { get; }
        public int ThrottleTimeMs { get; }

        public FetchResponseImpl(
            int version,
            IEnumerable<TopicFetchResponse> topics,
            int throttleTimeMs = -1)
        {
            Version = version;
            Topics = topics;
            ThrottleTimeMs = throttleTimeMs;
        }

        public static DecoderVersions<FetchResponse> Decode = new DecoderVersions<FetchResponse>(
            ApiKey.Fetch,
            reader => new FetchResponseImpl(
                version: 0,
                topics: reader.ReadList(TopicFetchResponseImpl.Versions[0])
            ),
            reader => new FetchResponseImpl(
                version: 1,
                throttleTimeMs: reader.ReadInt32(),
                topics: reader.ReadList(TopicFetchResponseImpl.Versions[0])
            ),
            reader => new FetchResponseImpl(
                version: 2,
                throttleTimeMs: reader.ReadInt32(),
                topics: reader.ReadList(TopicFetchResponseImpl.Versions[0])
            ),
            reader => new FetchResponseImpl(
                version: 3,
                throttleTimeMs: reader.ReadInt32(),
                topics: reader.ReadList(TopicFetchResponseImpl.Versions[0])
            )
        );
    }

    class TopicFetchResponseImpl : TopicFetchResponse
    {
        public string TopicName { get; }
        public IEnumerable<PartitionFetchResponse> Partitions { get; }

        public TopicFetchResponseImpl(
            string topicName,
            IEnumerable<PartitionFetchResponse> partitions)
        {
            TopicName = topicName;
            Partitions = partitions;
        }

        public static DecoderVersions<TopicFetchResponse> Versions = new DecoderVersions<TopicFetchResponse>(
            ApiKey.None,
            reader => new TopicFetchResponseImpl(
                topicName: reader.ReadString(),
                partitions: reader.ReadList(PartitionFetchResponseImpl.Versions[0])
            )
        );
    }

    class PartitionFetchResponseImpl : PartitionFetchResponse
    {
        public int Partition { get; }
        public KafkaError Error { get; }
        public long HighWatermark { get; }
        public IEnumerable<Message> MessageSet { get; } = new MessageSet();

        public PartitionFetchResponseImpl(
            int partition,
            KafkaError error,
            long highWatermark,
            IEnumerable<Message> messageSet)
        {
            Partition = partition;
            Error = error;
            HighWatermark = highWatermark;
            MessageSet = messageSet;
        }

        public static DecoderVersions<PartitionFetchResponse> Versions = new DecoderVersions<PartitionFetchResponse>(
            ApiKey.None,
            reader => new PartitionFetchResponseImpl(
                partition: reader.ReadInt32(),
                error: reader.ReadErrorCode(),
                highWatermark: reader.ReadInt64(),
                messageSet: reader.Read(reader.ReadInt32(), Protocol.MessageSet.Decode)
            )
        );
    }
}
