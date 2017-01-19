using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Kafka.Protocol
{
    public struct KafkaError
    {
        public short CodeNumber { get; }
        public ErrorCode Code => (ErrorCode)CodeNumber;
        public string Message => LookupMessage(CodeNumber);

        public bool CanRetry => LookupCanRetry(CodeNumber);

        public KafkaError(short code)
        {
            CodeNumber = code;
        }

        public KafkaError(ErrorCode code)
        {
            CodeNumber = (short)code;
        }

        public KafkaException AsException() => new KafkaException(this);

        public enum ErrorCode : short
        {
            Unknown = -1, // The server experienced an unexpected error when processing the request (CanRetry = False)
            None = 0, //  (CanRetry = False)
            OffsetOutOfRange = 1, // The requested offset is not within the range of offsets maintained by the server. (CanRetry = False)
            CorruptMessage = 2, // This message has failed its CRC checksum, exceeds the valid size, or is otherwise corrupt. (CanRetry = True)
            UnknownTopicOrPartition = 3, // This server does not host this topic-partition. (CanRetry = True)
            InvalidFetchSize = 4, // The requested fetch size is invalid. (CanRetry = False)
            LeaderNotAvailable = 5, // There is no leader for this topic-partition as we are in the middle of a leadership election. (CanRetry = True)
            NotLeaderForPartition = 6, // This server is not the leader for that topic-partition. (CanRetry = True)
            RequestTimedOut = 7, // The request timed out. (CanRetry = True)
            BrokerNotAvailable = 8, // The broker is not available. (CanRetry = False)
            ReplicaNotAvailable = 9, // The replica is not available for the requested topic-partition (CanRetry = False)
            MessageTooLarge = 10, // The request included a message larger than the max message size the server will accept. (CanRetry = False)
            StaleControllerEpoch = 11, // The controller moved to another broker. (CanRetry = False)
            OffsetMetadataTooLarge = 12, // The metadata field of the offset request was too large. (CanRetry = False)
            NetworkException = 13, // The server disconnected before a response was received. (CanRetry = True)
            GroupLoadInProgress = 14, // The coordinator is loading and hence can't process requests for this group. (CanRetry = True)
            GroupCoordinatorNotAvailable = 15, // The group coordinator is not available. (CanRetry = True)
            NotCoordinatorForGroup = 16, // This is not the correct coordinator for this group. (CanRetry = True)
            InvalidTopicException = 17, // The request attempted to perform an operation on an invalid topic. (CanRetry = False)
            RecordListTooLarge = 18, // The request included message batch larger than the configured segment size on the server. (CanRetry = False)
            NotEnoughReplicas = 19, // Messages are rejected since there are fewer in-sync replicas than required. (CanRetry = True)
            NotEnoughReplicasAfterAppend = 20, // Messages are written to the log, but to fewer in-sync replicas than required. (CanRetry = True)
            InvalidRequiredAcks = 21, // Produce request specified an invalid value for required acks. (CanRetry = False)
            IllegalGeneration = 22, // Specified group generation id is not valid. (CanRetry = False)
            InconsistentGroupProtocol = 23, // The group member's supported protocols are incompatible with those of existing members. (CanRetry = False)
            InvalidGroupId = 24, // The configured groupId is invalid (CanRetry = False)
            UnknownMemberId = 25, // The coordinator is not aware of this member. (CanRetry = False)
            InvalidSessionTimeout = 26, // The session timeout is not within the range allowed by the broker (as configured by group.min.session.timeout.ms and group.max.session.timeout.ms). (CanRetry = False)
            RebalanceInProgress = 27, // The group is rebalancing, so a rejoin is needed. (CanRetry = False)
            InvalidCommitOffsetSize = 28, // The committing offset data size is not valid (CanRetry = False)
            TopicAuthorizationFailed = 29, // Not authorized to access topics: [Topic authorization failed.] (CanRetry = False)
            GroupAuthorizationFailed = 30, // Not authorized to access group: Group authorization failed. (CanRetry = False)
            ClusterAuthorizationFailed = 31, // Cluster authorization failed. (CanRetry = False)
            InvalidTimestamp = 32, // The timestamp of the message is out of acceptable range. (CanRetry = False)
            UnsupportedSaslMechanism = 33, // The broker does not support the requested SASL mechanism. (CanRetry = False)
            IllegalSaslState = 34, // Request is not valid given the current SASL state. (CanRetry = False)
            UnsupportedVersion = 35, // The version of API is not supported. (CanRetry = False)
            TopicAlreadyExists = 36, // Topic with this name already exists. (CanRetry = False)
            InvalidPartitions = 37, // Number of partitions is invalid. (CanRetry = False)
            InvalidReplicationFactor = 38, // Replication-factor is invalid. (CanRetry = False)
            InvalidReplicaAssignment = 39, // Replica assignment is invalid. (CanRetry = False)
            InvalidConfig = 40, // Configuration is invalid. (CanRetry = False)
            NotController = 41, // This is not the correct controller for this cluster. (CanRetry = True)
            InvalidRequest = 42, // This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker. See the broker logs for more details. (CanRetry = False)
            UnsupportedForMessageFormat = 43, // The message format version on the broker does not support the request. (CanRetry = False)
        }

        public static KafkaError FromCode(short code)
        {
            return new KafkaError(code);
        }

        public static KafkaError Unknown => new KafkaError(ErrorCode.Unknown);
        public static KafkaError None => new KafkaError(ErrorCode.None);
        public static KafkaError OffsetOutOfRange => new KafkaError(ErrorCode.OffsetOutOfRange);
        public static KafkaError CorruptMessage => new KafkaError(ErrorCode.CorruptMessage);
        public static KafkaError UnknownTopicOrPartition => new KafkaError(ErrorCode.UnknownTopicOrPartition);
        public static KafkaError InvalidFetchSize => new KafkaError(ErrorCode.InvalidFetchSize);
        public static KafkaError LeaderNotAvailable => new KafkaError(ErrorCode.LeaderNotAvailable);
        public static KafkaError NotLeaderForPartition => new KafkaError(ErrorCode.NotLeaderForPartition);
        public static KafkaError RequestTimedOut => new KafkaError(ErrorCode.RequestTimedOut);
        public static KafkaError BrokerNotAvailable => new KafkaError(ErrorCode.BrokerNotAvailable);
        public static KafkaError ReplicaNotAvailable => new KafkaError(ErrorCode.ReplicaNotAvailable);
        public static KafkaError MessageTooLarge => new KafkaError(ErrorCode.MessageTooLarge);
        public static KafkaError StaleControllerEpoch => new KafkaError(ErrorCode.StaleControllerEpoch);
        public static KafkaError OffsetMetadataTooLarge => new KafkaError(ErrorCode.OffsetMetadataTooLarge);
        public static KafkaError NetworkException => new KafkaError(ErrorCode.NetworkException);
        public static KafkaError GroupLoadInProgress => new KafkaError(ErrorCode.GroupLoadInProgress);
        public static KafkaError GroupCoordinatorNotAvailable => new KafkaError(ErrorCode.GroupCoordinatorNotAvailable);
        public static KafkaError NotCoordinatorForGroup => new KafkaError(ErrorCode.NotCoordinatorForGroup);
        public static KafkaError InvalidTopicException => new KafkaError(ErrorCode.InvalidTopicException);
        public static KafkaError RecordListTooLarge => new KafkaError(ErrorCode.RecordListTooLarge);
        public static KafkaError NotEnoughReplicas => new KafkaError(ErrorCode.NotEnoughReplicas);
        public static KafkaError NotEnoughReplicasAfterAppend => new KafkaError(ErrorCode.NotEnoughReplicasAfterAppend);
        public static KafkaError InvalidRequiredAcks => new KafkaError(ErrorCode.InvalidRequiredAcks);
        public static KafkaError IllegalGeneration => new KafkaError(ErrorCode.IllegalGeneration);
        public static KafkaError InconsistentGroupProtocol => new KafkaError(ErrorCode.InconsistentGroupProtocol);
        public static KafkaError InvalidGroupId => new KafkaError(ErrorCode.InvalidGroupId);
        public static KafkaError UnknownMemberId => new KafkaError(ErrorCode.UnknownMemberId);
        public static KafkaError InvalidSessionTimeout => new KafkaError(ErrorCode.InvalidSessionTimeout);
        public static KafkaError RebalanceInProgress => new KafkaError(ErrorCode.RebalanceInProgress);
        public static KafkaError InvalidCommitOffsetSize => new KafkaError(ErrorCode.InvalidCommitOffsetSize);
        public static KafkaError TopicAuthorizationFailed => new KafkaError(ErrorCode.TopicAuthorizationFailed);
        public static KafkaError GroupAuthorizationFailed => new KafkaError(ErrorCode.GroupAuthorizationFailed);
        public static KafkaError ClusterAuthorizationFailed => new KafkaError(ErrorCode.ClusterAuthorizationFailed);
        public static KafkaError InvalidTimestamp => new KafkaError(ErrorCode.InvalidTimestamp);
        public static KafkaError UnsupportedSaslMechanism => new KafkaError(ErrorCode.UnsupportedSaslMechanism);
        public static KafkaError IllegalSaslState => new KafkaError(ErrorCode.IllegalSaslState);
        public static KafkaError UnsupportedVersion => new KafkaError(ErrorCode.UnsupportedVersion);
        public static KafkaError TopicAlreadyExists => new KafkaError(ErrorCode.TopicAlreadyExists);
        public static KafkaError InvalidPartitions => new KafkaError(ErrorCode.InvalidPartitions);
        public static KafkaError InvalidReplicationFactor => new KafkaError(ErrorCode.InvalidReplicationFactor);
        public static KafkaError InvalidReplicaAssignment => new KafkaError(ErrorCode.InvalidReplicaAssignment);
        public static KafkaError InvalidConfig => new KafkaError(ErrorCode.InvalidConfig);
        public static KafkaError NotController => new KafkaError(ErrorCode.NotController);
        public static KafkaError InvalidRequest => new KafkaError(ErrorCode.InvalidRequest);
        public static KafkaError UnsupportedForMessageFormat => new KafkaError(ErrorCode.UnsupportedForMessageFormat);

        private static bool LookupCanRetry(short codeNumber)
        {
            switch ((ErrorCode)codeNumber)
            {
                case ErrorCode.Unknown:
                    return false;
                case ErrorCode.None:
                    return false;
                case ErrorCode.OffsetOutOfRange:
                    return false;
                case ErrorCode.CorruptMessage:
                    return true;
                case ErrorCode.UnknownTopicOrPartition:
                    return true;
                case ErrorCode.InvalidFetchSize:
                    return false;
                case ErrorCode.LeaderNotAvailable:
                    return true;
                case ErrorCode.NotLeaderForPartition:
                    return true;
                case ErrorCode.RequestTimedOut:
                    return true;
                case ErrorCode.BrokerNotAvailable:
                    return false;
                case ErrorCode.ReplicaNotAvailable:
                    return false;
                case ErrorCode.MessageTooLarge:
                    return false;
                case ErrorCode.StaleControllerEpoch:
                    return false;
                case ErrorCode.OffsetMetadataTooLarge:
                    return false;
                case ErrorCode.NetworkException:
                    return true;
                case ErrorCode.GroupLoadInProgress:
                    return true;
                case ErrorCode.GroupCoordinatorNotAvailable:
                    return true;
                case ErrorCode.NotCoordinatorForGroup:
                    return true;
                case ErrorCode.InvalidTopicException:
                    return false;
                case ErrorCode.RecordListTooLarge:
                    return false;
                case ErrorCode.NotEnoughReplicas:
                    return true;
                case ErrorCode.NotEnoughReplicasAfterAppend:
                    return true;
                case ErrorCode.InvalidRequiredAcks:
                    return false;
                case ErrorCode.IllegalGeneration:
                    return false;
                case ErrorCode.InconsistentGroupProtocol:
                    return false;
                case ErrorCode.InvalidGroupId:
                    return false;
                case ErrorCode.UnknownMemberId:
                    return false;
                case ErrorCode.InvalidSessionTimeout:
                    return false;
                case ErrorCode.RebalanceInProgress:
                    return false;
                case ErrorCode.InvalidCommitOffsetSize:
                    return false;
                case ErrorCode.TopicAuthorizationFailed:
                    return false;
                case ErrorCode.GroupAuthorizationFailed:
                    return false;
                case ErrorCode.ClusterAuthorizationFailed:
                    return false;
                case ErrorCode.InvalidTimestamp:
                    return false;
                case ErrorCode.UnsupportedSaslMechanism:
                    return false;
                case ErrorCode.IllegalSaslState:
                    return false;
                case ErrorCode.UnsupportedVersion:
                    return false;
                case ErrorCode.TopicAlreadyExists:
                    return false;
                case ErrorCode.InvalidPartitions:
                    return false;
                case ErrorCode.InvalidReplicationFactor:
                    return false;
                case ErrorCode.InvalidReplicaAssignment:
                    return false;
                case ErrorCode.InvalidConfig:
                    return false;
                case ErrorCode.NotController:
                    return true;
                case ErrorCode.InvalidRequest:
                    return false;
                case ErrorCode.UnsupportedForMessageFormat:
                    return false;
                default:
                    return false;
            }
        }

        private static string LookupMessage(short codeNumber)
        {
            switch ((ErrorCode)codeNumber)
            {
                case ErrorCode.Unknown:
                    return "The server experienced an unexpected error when processing the request";
                case ErrorCode.None:
                    return "	";
                case ErrorCode.OffsetOutOfRange:
                    return "The requested offset is not within the range of offsets maintained by the server.";
                case ErrorCode.CorruptMessage:
                    return "This message has failed its CRC checksum, exceeds the valid size, or is otherwise corrupt.";
                case ErrorCode.UnknownTopicOrPartition:
                    return "This server does not host this topic-partition.";
                case ErrorCode.InvalidFetchSize:
                    return "The requested fetch size is invalid.";
                case ErrorCode.LeaderNotAvailable:
                    return "There is no leader for this topic-partition as we are in the middle of a leadership election.";
                case ErrorCode.NotLeaderForPartition:
                    return "This server is not the leader for that topic-partition.";
                case ErrorCode.RequestTimedOut:
                    return "The request timed out.";
                case ErrorCode.BrokerNotAvailable:
                    return "The broker is not available.";
                case ErrorCode.ReplicaNotAvailable:
                    return "The replica is not available for the requested topic-partition";
                case ErrorCode.MessageTooLarge:
                    return "The request included a message larger than the max message size the server will accept.";
                case ErrorCode.StaleControllerEpoch:
                    return "The controller moved to another broker.";
                case ErrorCode.OffsetMetadataTooLarge:
                    return "The metadata field of the offset request was too large.";
                case ErrorCode.NetworkException:
                    return "The server disconnected before a response was received.";
                case ErrorCode.GroupLoadInProgress:
                    return "The coordinator is loading and hence can't process requests for this group.";
                case ErrorCode.GroupCoordinatorNotAvailable:
                    return "The group coordinator is not available.";
                case ErrorCode.NotCoordinatorForGroup:
                    return "This is not the correct coordinator for this group.";
                case ErrorCode.InvalidTopicException:
                    return "The request attempted to perform an operation on an invalid topic.";
                case ErrorCode.RecordListTooLarge:
                    return "The request included message batch larger than the configured segment size on the server.";
                case ErrorCode.NotEnoughReplicas:
                    return "Messages are rejected since there are fewer in-sync replicas than required.";
                case ErrorCode.NotEnoughReplicasAfterAppend:
                    return "Messages are written to the log, but to fewer in-sync replicas than required.";
                case ErrorCode.InvalidRequiredAcks:
                    return "Produce request specified an invalid value for required acks.";
                case ErrorCode.IllegalGeneration:
                    return "Specified group generation id is not valid.";
                case ErrorCode.InconsistentGroupProtocol:
                    return "The group member's supported protocols are incompatible with those of existing members.";
                case ErrorCode.InvalidGroupId:
                    return "The configured groupId is invalid";
                case ErrorCode.UnknownMemberId:
                    return "The coordinator is not aware of this member.";
                case ErrorCode.InvalidSessionTimeout:
                    return "The session timeout is not within the range allowed by the broker (as configured by group.min.session.timeout.ms and group.max.session.timeout.ms).";
                case ErrorCode.RebalanceInProgress:
                    return "The group is rebalancing, so a rejoin is needed.";
                case ErrorCode.InvalidCommitOffsetSize:
                    return "The committing offset data size is not valid";
                case ErrorCode.TopicAuthorizationFailed:
                    return "Not authorized to access topics: [Topic authorization failed.]";
                case ErrorCode.GroupAuthorizationFailed:
                    return "Not authorized to access group: Group authorization failed.";
                case ErrorCode.ClusterAuthorizationFailed:
                    return "Cluster authorization failed.";
                case ErrorCode.InvalidTimestamp:
                    return "The timestamp of the message is out of acceptable range.";
                case ErrorCode.UnsupportedSaslMechanism:
                    return "The broker does not support the requested SASL mechanism.";
                case ErrorCode.IllegalSaslState:
                    return "Request is not valid given the current SASL state.";
                case ErrorCode.UnsupportedVersion:
                    return "The version of API is not supported.";
                case ErrorCode.TopicAlreadyExists:
                    return "Topic with this name already exists.";
                case ErrorCode.InvalidPartitions:
                    return "Number of partitions is invalid.";
                case ErrorCode.InvalidReplicationFactor:
                    return "Replication-factor is invalid.";
                case ErrorCode.InvalidReplicaAssignment:
                    return "Replica assignment is invalid.";
                case ErrorCode.InvalidConfig:
                    return "Configuration is invalid.";
                case ErrorCode.NotController:
                    return "This is not the correct controller for this cluster.";
                case ErrorCode.InvalidRequest:
                    return "This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker. See the broker logs for more details.";
                case ErrorCode.UnsupportedForMessageFormat:
                    return "The message format version on the broker does not support the request.";
                default:
                    return $"The server returned an unexpected error code {codeNumber}";
            }
        }


    }
}
