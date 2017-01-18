using System;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Protocol
{
    public interface OffsetsResponse
    {
        int Version { get; }
        IEnumerable<TopicOffsetsResponse> Topics { get; }
    }

    public interface TopicOffsetsResponse
    {
        string TopicName { get; }
        IEnumerable<PartitionOffsetsResponse> Partitions { get; }
    }

    public interface PartitionOffsetsResponse
    {
        int Partition { get; }
        short ErrorCode { get; }
        long Timestamp { get; }
        long Offset { get; }
        [Obsolete("Deprecated at version 0.10.1.0, new versions use only Offset")]
        IEnumerable<long> Offsets { get; }
    }

    class OffsetsResponseImpl : OffsetsResponse
    {
        public int Version { get; }
        public IEnumerable<TopicOffsetsResponse> Topics { get; }

        public OffsetsResponseImpl(
            int version,
            IEnumerable<TopicOffsetsResponse> topics)
        {
            Version = version;
            Topics = topics;
        }

        public static DecoderVersions<OffsetsResponse> Decode = new DecoderVersions<OffsetsResponse>(
            ApiKey.Offsets,
            reader => new OffsetsResponseImpl(
                version: 0,
                topics: reader.ReadList(TopicOffsetsResponseImpl.Versions[0])
            ),
            reader => new OffsetsResponseImpl(
                version: 1,
                topics: reader.ReadList(TopicOffsetsResponseImpl.Versions[1])
            )
        );
    }

    class TopicOffsetsResponseImpl : TopicOffsetsResponse
    {
        public string TopicName { get; }
        public IEnumerable<PartitionOffsetsResponse> Partitions { get; }

        public TopicOffsetsResponseImpl(
            string topicName,
            IEnumerable<PartitionOffsetsResponse> partitions)
        {
            TopicName = topicName;
            Partitions = partitions;
        }

        public static DecoderVersions<TopicOffsetsResponse> Versions = new DecoderVersions<TopicOffsetsResponse>(
            ApiKey.None,
            reader => new TopicOffsetsResponseImpl(
                topicName: reader.ReadString(),
                partitions: reader.ReadList(PartitionOffsetsResponseImpl.Versions[0])
            ),
            reader => new TopicOffsetsResponseImpl(
                topicName: reader.ReadString(),
                partitions: reader.ReadList(PartitionOffsetsResponseImpl.Versions[1])
            )
        );
    }

    class PartitionOffsetsResponseImpl : PartitionOffsetsResponse
    {
        public int Partition { get; }
        public short ErrorCode { get; }
        public long Timestamp { get; }
        public long Offset { get; }
        public IEnumerable<long> Offsets { get; }

        public PartitionOffsetsResponseImpl(
            int partition,
            short errorCode,
            long timestamp = -1,
            long offset = -1,
            IEnumerable<long> offsets = null)
        {
            Partition = partition;
            ErrorCode = errorCode;
            Timestamp = timestamp;
            Offset = offset > 0 ? offset : (offsets?.FirstOrDefault() ?? offset);
            Offsets = offsets ?? new[] { offset };
        }

        public static DecoderVersions<PartitionOffsetsResponse> Versions = new DecoderVersions<PartitionOffsetsResponse>(
            ApiKey.None,
            reader => new PartitionOffsetsResponseImpl(
                partition: reader.ReadInt32(),
                errorCode: reader.ReadInt16(),
                offsets: reader.ReadList(r => r.ReadInt64())
            ),
            reader => new PartitionOffsetsResponseImpl(
                partition: reader.ReadInt32(),
                errorCode: reader.ReadInt16(),
                timestamp: reader.ReadInt64(),
                offset: reader.ReadInt64()
            )
        );
    }
}
