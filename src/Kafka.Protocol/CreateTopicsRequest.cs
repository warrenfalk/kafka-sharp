using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Kafka.Protocol
{
    public class CreateTopicsRequest : Request
    {
        public ApiKey ApiKey => Protocol.ApiKey.CreateTopics;
        public short ApiVersion { get; }

        public List<CreateTopicRequest> Topics { get; } = new List<CreateTopicRequest>();
        public int Timeout { get; set; }

        public CreateTopicsRequest(short apiVersion)
        {
            ApiVersion = apiVersion;
        }

        public void WriteTo(ProtocolWriter writer)
        {
            switch (ApiVersion)
            {
                case 0:
                    writer
                        .WriteList(Topics, Protocol.CreateTopicRequest.Encode)
                        .WriteInt32(Timeout);
                    break;
                default:
                    throw new UnknownApiVersionException(ApiVersion, ApiKey);
            }
        }
    }

    public class CreateTopicRequest
    {
        public string TopicName { get; set; }
        public int NumPartitions { get; set; } = -1;
        public short ReplicationFactor { get; set; } = -1;
        public ReplicaAssignments ReplicaAssignment { get; } = new ReplicaAssignments();
        public Configuration ConfigValues { get; } = new Configuration();

        public static ProtocolWriter Encode(CreateTopicRequest value, ProtocolWriter writer)
            => writer
                .WriteString(value.TopicName)
                .WriteInt32(value.NumPartitions)
                .WriteInt16(value.ReplicationFactor)
                .WriteList(value.ReplicaAssignment, Protocol.ReplicaAssignment.Encode)
                .WriteList<ConfigValue>(value.ConfigValues, ConfigValue.Encode);
    }

    public class ReplicaAssignments : IEnumerable<ReplicaAssignment>, ICollection<ReplicaAssignment>
    {
        private Dictionary<int, ReplicaAssignment> Assignments { get; } = new Dictionary<int, ReplicaAssignment>();

        public int Count => Assignments.Count;

        public bool IsReadOnly => false;

        public void Add(ReplicaAssignment item) => Assignments.Add(item.PartitionId, item);

        public void Add(int partitionId, params int[] replicas) => Add(new ReplicaAssignment(partitionId, replicas));

        public void Clear() => Assignments.Clear();

        public bool Contains(ReplicaAssignment item) => Assignments.ContainsKey(item.PartitionId) && Assignments[item.PartitionId].Replicas.SequenceEqual(item.Replicas);

        public void CopyTo(ReplicaAssignment[] array, int arrayIndex)
        {
            int i = arrayIndex;
            foreach (var x in this)
                array[i++] = x;
        }

        public IEnumerator<ReplicaAssignment> GetEnumerator() => Assignments.OrderBy(e => e.Key).Select(e => e.Value).GetEnumerator();

        public bool Remove(ReplicaAssignment item) => Assignments.Remove(item.PartitionId);

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    public class ReplicaAssignment
    {
        public int PartitionId { get; set; }
        public List<int> Replicas { get; } = new List<int>();

        public ReplicaAssignment() { }

        public ReplicaAssignment(int partitionId, params int[] replicas)
        {
            PartitionId = partitionId;
            Replicas.AddRange(replicas);
        }

        public static ProtocolWriter Encode(ReplicaAssignment value, ProtocolWriter writer)
            => writer
                .WriteInt32(value.PartitionId)
                .WriteList(value.Replicas, Protocol.Encode.Int32);
    }

    public class Configuration : IEnumerable<ConfigValue>, ICollection<ConfigValue>, IDictionary<string, string>
    {
        private Dictionary<string, string> ConfigValues = new Dictionary<string, string>();

        public string this[string key]
        {
            get { return ((IDictionary<string, string>)ConfigValues)[key]; }
            set { ((IDictionary<string, string>)ConfigValues)[key] = value; }
        }

        public int Count => ((IDictionary<string, string>)ConfigValues).Count;

        public bool IsReadOnly => false;

        public ICollection<string> Keys => ConfigValues.Keys;

        public ICollection<string> Values => ConfigValues.Values;

        public void Add(ConfigValue item) => ConfigValues.Add(item.Key, item.Value);

        public void Add(KeyValuePair<string, string> item) => ConfigValues.Add(item.Key, item.Value);

        public void Add(string key, string value) => ConfigValues.Add(key, value);

        public void Clear() => ConfigValues.Clear();

        public bool Contains(ConfigValue item) => Contains(item);

        public bool Contains(KeyValuePair<string, string> item) => (ConfigValues as IDictionary<string, string>).Contains(item);

        public bool ContainsKey(string key) => ConfigValues.ContainsKey(key);

        public void CopyTo(ConfigValue[] array, int arrayIndex)
        {
            int i = arrayIndex;
            foreach (var e in this)
                array[i++] = e;
        }

        public void CopyTo(KeyValuePair<string, string>[] array, int arrayIndex) => (ConfigValues as IDictionary<string, string>).CopyTo(array, arrayIndex);

        public IEnumerator<ConfigValue> GetEnumerator() => ConfigValues.OrderBy(k => k.Key).Select(k => (ConfigValue)k).GetEnumerator();

        public bool Remove(KeyValuePair<string, string> item) => (ConfigValues as IDictionary<string, string>).Remove(item);

        public bool Remove(ConfigValue item) => Remove((KeyValuePair<string, string>)item);

        public bool Remove(string key) => ConfigValues.Remove(key);

        public bool TryGetValue(string key, out string value) => ConfigValues.TryGetValue(key, out value);

        IEnumerator<KeyValuePair<string, string>> IEnumerable<KeyValuePair<string, string>>.GetEnumerator() => ConfigValues.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    public struct ConfigValue
    {
        public string Key { get; }
        public string Value { get; }

        public ConfigValue(string key, string value)
        {
            Key = key;
            Value = value;
        }

        public static ProtocolWriter Encode(ConfigValue value, ProtocolWriter writer)
            => writer
                .WriteString(value.Key)
                .WriteString(value.Value);

        public static implicit operator KeyValuePair<string, string>(ConfigValue item) => new KeyValuePair<string, string>(item.Key, item.Value);
        public static implicit operator ConfigValue(KeyValuePair<string, string> item) => new ConfigValue(item.Key, item.Value);
    }
}
