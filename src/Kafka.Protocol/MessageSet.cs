using Force.Crc32;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Collections;
using System.Text;

namespace Kafka.Protocol
{
    public class MessageSet : IEnumerable<Message>
    {
        public List<Message> Messages { get; } = new List<Message>();

        public static void Encode(MessageSet set, ProtocolStreamWriter writer)
        {
            int size = 0;
            foreach (var message in set.Messages)
                size += MessageImpl.CalcEntrySize(message);
            writer.WriteInt32(size);
            foreach (var message in set.Messages)
                MessageImpl.Encode(message, writer);
        }

        public static MessageSet Decode(ProtocolStreamReader reader)
        {
            var messageSet = new MessageSet();
            while (!reader.IsAtEnd())
            {
                var message = MessageImpl.Decode(reader);
                messageSet.Add(message);
            }
            return messageSet;
        }

        public IEnumerator<Message> GetEnumerator() => Messages.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => Messages.GetEnumerator();

        public void Add(Message message) => Messages.Add(message);

        public void Add(string key, string value) => Messages.Add(new MessageImpl(key, value));

        public void Add(byte[] key, byte[] value) => Messages.Add(new MessageImpl(key, value));
    }

    public interface Message
    {
        long Offset { get; }
        sbyte Version { get; } // aka MagicByte
        Compression Compression { get; }
        long Timestamp { get; }
        ArraySegment<byte> Key { get; }
        ArraySegment<byte> Value { get; }
    }

    public class MessageImpl : Message
    {
        public long Offset { get; set; }
        public sbyte Version { get; set; } = 0;
        public Compression Compression { get; set; }
        public long Timestamp { get; set; }
        public ArraySegment<byte> Key { get; set; }
        public ArraySegment<byte> Value { get; set; }

        public MessageImpl()
        { }

        public MessageImpl(string key, string value)
            : this(
                  key: key != null ? Encoding.UTF8.GetBytes(key) : null,
                  value: value != null ? Encoding.UTF8.GetBytes(value) : null)
        { }

        public MessageImpl(byte[] key, byte[] value)
        {
            Key = new ArraySegment<byte>(key);
            Value = new ArraySegment<byte>(value);
        }

        public static Message Decode(ProtocolStreamReader reader)
        {
            throw new NotImplementedException();
        }

        public static void Encode(Message message, ProtocolStreamWriter writer)
        {
            // This is a little bit complicated because the protocol inconveniently requires us to send the CRC of the data before the data itself
            // So we have to first encode everything that needs to be encoded to some buffers
            // Then we'll calculate the CRC using those buffers
            // Then finally we send the data

            var key = message.Key;
            var value = message.Value;
            var haveKey = key.Count > 0;
            var haveValue = value.Count > 0;
            var version = message.Version;
            var headerSize = CalcHeaderSize(message);
            var attributes = (sbyte)message.Compression;
            var messageSize = CalcMessageSize(message);

            writer.WriteInt64(message.Offset);
            writer.WriteInt32(messageSize);

            var headerBytes = new byte[headerSize];
            Protocol.Encode.Int8(message.Version, headerBytes, 0);
            Protocol.Encode.Int8(attributes, headerBytes, 1);
            if (version >= 1)
                Protocol.Encode.Int64(message.Timestamp, headerBytes, 2);

            var keySize = new byte[4];
            Protocol.Encode.Int32(key.Count, keySize, 0);
            var valueSize = new byte[4];
            Protocol.Encode.Int32(value.Count, valueSize, 0);

            // Now calculate the CRC
            var crc32 = (uint)0;
            crc32 = Crc32Algorithm.Append(crc32, headerBytes);
            crc32 = Crc32Algorithm.Append(crc32, keySize);
            if (haveKey)
                crc32 = Crc32Algorithm.Append(crc32, key.Array, key.Offset, key.Count);
            crc32 = Crc32Algorithm.Append(crc32, valueSize);
            if (haveValue)
                crc32 = Crc32Algorithm.Append(crc32, value.Array, value.Offset, value.Count);

            writer.WriteInt32((int)crc32);
            writer.WriteRaw(headerBytes);
            writer.WriteRaw(keySize);
            if (haveKey)
                writer.WriteRaw(key.Array, key.Offset, key.Count);
            writer.WriteRaw(valueSize);
            if (haveValue)
                writer.WriteRaw(value.Array, value.Offset, value.Count);
        }

        public const int FixedEntrySize = 8 /* Offset */ + 4 /* Message Size */;
        public const int FixedMessageSize = 4 /* CRC */ + 4 /* Key.Length */ + 4 /* Value.Length */;

        public static int CalcHeaderSize(Message message) => message.Version == 0 ? 2 : 10;

        public static int CalcMessageSize(Message message)
        {
            var key = message.Key;
            var value = message.Value;
            return FixedMessageSize + CalcHeaderSize(message) + key.Count + value.Count;
        }

        public static int CalcEntrySize(Message message)
        {
            return CalcMessageSize(message) + FixedEntrySize;
        }
    }
}
