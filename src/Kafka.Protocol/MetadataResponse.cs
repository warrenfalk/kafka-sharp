using System;
using System.Collections.Generic;

namespace Kafka.Protocol
{
    public interface MetadataResponse
    {
        int Version { get; }
        IEnumerable<BrokerMetadata> Brokers { get; }
        int ControllerId { get; }
        IEnumerable<TopicMetadata> Topics { get; }
    }

    public interface BrokerMetadata
    {
        int NodeId { get; }
        string HostName { get; }
        int Port { get; }
        string Rack { get; }
    }

    public interface TopicMetadata
    {
        short TopicErrorCode { get; }
        string TopicName { get; }
        bool IsInternal { get; }
        IEnumerable<PartitionMetadata> Partitions { get; }
    }

    public interface PartitionMetadata
    {
        short PartitionErrorCode { get; }
        int PartitionId { get; }
        int Leader { get; }
        IEnumerable<int> Replicas { get; }
        IEnumerable<int> Isr { get; }
    }

    class MetadataResponseImpl : MetadataResponse
    {
        public int Version { get; }
        public IEnumerable<BrokerMetadata> Brokers { get; }
        public int ControllerId { get; }
        public IEnumerable<TopicMetadata> Topics { get; }

        public MetadataResponseImpl(
            int version,
            IEnumerable<BrokerMetadata> brokers,
            IEnumerable<TopicMetadata> topics,
            int controllerId = -1)
        {
            Version = version;
            Brokers = brokers;
            Topics = topics;
            ControllerId = controllerId;
        }

        public static DecoderVersions<MetadataResponse> Decode = new DecoderVersions<MetadataResponse>(
            reader => new MetadataResponseImpl(
                version: 0,
                brokers: reader.ReadList(BrokerMetadataImpl.Versions[0]),
                topics: reader.ReadList(TopicMetadataImpl.Versions[0])
            ),
            reader => new MetadataResponseImpl(
                version: 0,
                brokers: reader.ReadList(BrokerMetadataImpl.Versions[1]),
                controllerId: reader.ReadInt32(),
                topics: reader.ReadList(TopicMetadataImpl.Versions[1])
            )
        );
    }

    class BrokerMetadataImpl : BrokerMetadata
    {
        public int NodeId { get; }
        public string HostName { get; }
        public int Port { get; }
        public string Rack { get; }

        public BrokerMetadataImpl(
            int nodeId,
            string hostName,
            int port,
            string rack = null)
        {
            NodeId = nodeId;
            HostName = hostName;
            Port = port;
            Rack = rack;
        }

        public static DecoderVersions<BrokerMetadata> Versions = new DecoderVersions<BrokerMetadata>(
            reader => new BrokerMetadataImpl(
                nodeId: reader.ReadInt32(),
                hostName: reader.ReadString(),
                port: reader.ReadInt32()
            ),
            reader => new BrokerMetadataImpl(
                nodeId: reader.ReadInt32(),
                hostName: reader.ReadString(),
                port: reader.ReadInt32(),
                rack: reader.ReadNullableString()
            )
        );
    }

    class TopicMetadataImpl : TopicMetadata
    {
        public short TopicErrorCode { get; }
        public string TopicName { get; }
        public bool IsInternal { get; }
        public IEnumerable<PartitionMetadata> Partitions { get; }

        public TopicMetadataImpl(
            short topicErrorCode,
            string topicName,
            IEnumerable<PartitionMetadata> partitions,
            bool isInternal = false)
        {
            TopicErrorCode = topicErrorCode;
            TopicName = topicName;
            Partitions = partitions;
            IsInternal = isInternal;
        }

        public static DecoderVersions<TopicMetadata> Versions = new DecoderVersions<TopicMetadata>(
            reader => new TopicMetadataImpl(
                topicErrorCode: reader.ReadInt16(),
                topicName: reader.ReadString(),
                partitions: reader.ReadList(PartitionMetadataImpl.Versions[0])
            ),
            reader => new TopicMetadataImpl(
                topicErrorCode: reader.ReadInt16(),
                topicName: reader.ReadString(),
                isInternal: reader.ReadBoolean(),
                partitions: reader.ReadList(PartitionMetadataImpl.Versions[0])
            )
        );
    }

    class PartitionMetadataImpl : PartitionMetadata
    {
        public short PartitionErrorCode { get; }
        public int PartitionId { get; }
        public int Leader { get; }
        public IEnumerable<int> Replicas { get; }
        public IEnumerable<int> Isr { get; }

        public PartitionMetadataImpl(
            short partitionErrorCode,
            int partitionId,
            int leader,
            IEnumerable<int> replicas,
            IEnumerable<int> isr
            )
        {
            PartitionErrorCode = partitionErrorCode;
            PartitionId = partitionId;
            Leader = leader;
            Replicas = replicas;
            Isr = isr;
        }

        public static DecoderVersions<PartitionMetadata> Versions = new DecoderVersions<PartitionMetadata>(
            reader => new PartitionMetadataImpl(
                partitionErrorCode: reader.ReadInt16(),
                partitionId: reader.ReadInt32(),
                leader: reader.ReadInt32(),
                replicas: reader.ReadList(Protocol.Decode.Int32),
                isr: reader.ReadList(Protocol.Decode.Int32)
            )
        );
    }


}
