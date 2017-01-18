using System;
using System.Collections.Generic;

namespace Kafka.Protocol
{
    public interface GroupCoordinatorResponse
    {
        int Version { get; }
        short ErrorCode { get; }
        CoordinatorResponse Coordinator { get; }
    }

    public interface CoordinatorResponse
    {
        int NodeId { get; }
        string Host { get; }
        int Port { get; }
    }

    class GroupCoordinatorResponseImpl : GroupCoordinatorResponse
    {
        public int Version { get; }
        public short ErrorCode { get; }
        public CoordinatorResponse Coordinator { get; }

        public GroupCoordinatorResponseImpl(
            int version,
            short errorCode,
            CoordinatorResponse coordinator)
        {
            Version = version;
            ErrorCode = errorCode;
            Coordinator = coordinator;
        }

        public static DecoderVersions<GroupCoordinatorResponse> Decode = new DecoderVersions<GroupCoordinatorResponse>(
            ApiKey.GroupCoordinator,
            reader => new GroupCoordinatorResponseImpl(
                version: 0,
                errorCode: reader.ReadInt16(),
                coordinator: reader.Read(Protocol.CoordinatorResponseImpl.Versions[0])
            )
        );
    }

    class CoordinatorResponseImpl : CoordinatorResponse
    {
        public int NodeId { get; }
        public string Host { get; }
        public int Port { get; }

        public CoordinatorResponseImpl(
            int nodeId,
            string host,
            int port)
        {
            NodeId = nodeId;
            Host = host;
            Port = port;
        }

        public static DecoderVersions<CoordinatorResponse> Versions = new DecoderVersions<CoordinatorResponse>(
            ApiKey.None,
            reader => new CoordinatorResponseImpl(
                nodeId: reader.ReadInt32(),
                host: reader.ReadString(),
                port: reader.ReadInt32()
            )
        );
    }
}
