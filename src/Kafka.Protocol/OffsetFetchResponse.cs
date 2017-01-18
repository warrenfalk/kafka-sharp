using System;
using System.Collections.Generic;

namespace Kafka.Protocol
{
    public interface OffsetFetchResponse
    {
        int Version { get; }
        IEnumerable<TopicOffsetFetchResponse> Topics { get; }
    }

    public interface TopicOffsetFetchResponse
    {
        string TopicName { get; }
        IEnumerable<PartitionOffsetFetchResponse> Partitions { get; }
    }

    public interface PartitionOffsetFetchResponse
    {
        int Partition { get; }
        long Offset { get; }
        string Metadata { get; }
        short ErrorCode { get; }
    }

    class OffsetFetchResponseImpl : OffsetFetchResponse
    {
        public int Version { get; }
        public IEnumerable<TopicOffsetFetchResponse> Topics { get; }

        public OffsetFetchResponseImpl(
            int version,
            IEnumerable<TopicOffsetFetchResponse> topics)
        {
            Version = version;
            Topics = topics;
        }

        public static DecoderVersions<OffsetFetchResponse> Decode = new DecoderVersions<OffsetFetchResponse>(
            ApiKey.OffsetFetch,
            reader => new OffsetFetchResponseImpl(
                version: 0,
                topics: reader.ReadList(TopicOffsetFetchResponseImpl.Versions[0])
            ),
            reader => new OffsetFetchResponseImpl(
                version: 1,
                topics: reader.ReadList(TopicOffsetFetchResponseImpl.Versions[0])
            )
        );
    }

    class TopicOffsetFetchResponseImpl : TopicOffsetFetchResponse
    {
        public string TopicName { get; }
        public IEnumerable<PartitionOffsetFetchResponse> Partitions { get; }

        public TopicOffsetFetchResponseImpl(
            string topicName,
            IEnumerable<PartitionOffsetFetchResponse> partitions)
        {
            TopicName = topicName;
            Partitions = partitions;
        }

        public static DecoderVersions<TopicOffsetFetchResponse> Versions = new DecoderVersions<TopicOffsetFetchResponse>(
            ApiKey.None,
            reader => new TopicOffsetFetchResponseImpl(
                topicName: reader.ReadString(),
                partitions: reader.ReadList(PartitionOffsetFetchResponseImpl.Versions[0])
            )
        );
    }

    class PartitionOffsetFetchResponseImpl : PartitionOffsetFetchResponse
    {
        public int Partition { get; }
        public long Offset { get; }
        public string Metadata { get; }
        public short ErrorCode { get; }

        public PartitionOffsetFetchResponseImpl(
            int partition,
            long offset,
            string metadata,
            short errorCode)
        {
            Partition = partition;
            Offset = offset;
            Metadata = metadata;
            ErrorCode = errorCode;
        }

        public static DecoderVersions<PartitionOffsetFetchResponse> Versions = new DecoderVersions<PartitionOffsetFetchResponse>(
            ApiKey.None,
            reader => new PartitionOffsetFetchResponseImpl(
                partition: reader.ReadInt32(),
                offset: reader.ReadInt64(),
                metadata: reader.ReadNullableString(),
                errorCode: reader.ReadInt16()
            )
        );
    }
}
