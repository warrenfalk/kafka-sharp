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
        private List<Message> Messages { get; } = new List<Message>();

        public static void Encode(MessageSet set, ProtocolWriter writer)
        {
            int size = 0;
            foreach (var message in set.Messages)
                size += MessageImpl.CalcEntrySize(message);
            writer.WriteInt32(size);
            foreach (var message in set.Messages)
                MessageImpl.Encode(message, writer);
        }

        public static IEnumerable<Message> Decode(ProtocolReader reader)
        {
            var messageSet = new MessageSet();
            for(;;)
            {
                var message = MessageImpl.Decode(reader);
                if (message == null)
                    break;
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
        BinaryValue Key { get; }
        BinaryValue Value { get; }
    }

    public class MessageImpl : Message
    {
        public long Offset { get; set; }
        public sbyte Version { get; set; } = 0;
        public Compression Compression { get; set; }
        public long Timestamp { get; set; }
        public BinaryValue Key { get; set; }
        public BinaryValue Value { get; set; }

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

        public static Message Decode(ProtocolReader reader)
        {
            // The decoding of Messages is implemented in a way which is peculiar compared to other decode
            // functions because messages are decoded from a stream which may end abruptly in the middle
            // of a message, and so we need to hande that.
            // We handle that with the ReadRaw() method which can return a slice smaller than we ask for
            
            // need 12 bytes for a message header
            var header = reader.ReadRaw(12);
            if (header.Length < 12)
                return null; // the entire message is not available
            var offset = Protocol.Decode.Int64(header);
            var size = Protocol.Decode.Int32(header.At(8));

            // that gives us the size, so now we try to read that size
            var subd = reader.ReadRaw(size);
            if (subd.Length < size)
                return null; // the entire message is not available

            reader = new ProtocolReader(subd);
            var crc = reader.ReadInt32();
            var magic = reader.ReadInt8();
            var attributes = reader.ReadInt8();
            if (attributes != 0)
                throw new NotImplementedException("TODO: implement compression in MessageImpl.Decode()");
            var timestamp = (magic >= 1)
                ? reader.ReadInt64()
                : -1;
            var keySize = reader.ReadInt32();
            var key = keySize < 0 ? null : new BinaryValue(reader.ReadRaw(keySize));
            var valueSize = reader.ReadInt32();
            var value = valueSize < 0 ? null : new BinaryValue(reader.ReadRaw(valueSize));
            return new MessageImpl
            {
                Offset = offset,
                Version = magic,
                Compression = Compression.None,
                Timestamp = timestamp,
                Key = key,
                Value = value,
            };
        }

        public static void Encode(Message message, ProtocolWriter writer)
        {
            // This is a little bit complicated because the protocol inconveniently requires us to send the CRC of the data before the data itself
            // So we have to first encode everything that needs to be encoded to some buffers
            // Then we'll calculate the CRC using those buffers
            // Then finally we send the data

            var key = message.Key;
            var value = message.Value;
            var haveKey = key != null;
            var haveValue = value != null;
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
            Protocol.Encode.Int32(key?.Length ?? -1, keySize, 0);
            var valueSize = new byte[4];
            Protocol.Encode.Int32(value?.Length ?? -1, valueSize, 0);

            // Now calculate the CRC
            var crc32 = (uint)0;
            crc32 = Crc32Algorithm.Append(crc32, headerBytes);
            crc32 = Crc32Algorithm.Append(crc32, keySize);
            if (haveKey)
                crc32 = Crc32Algorithm.Append(crc32, key.Slice.Buffer, key.Slice.Offset, key.Slice.Length);
            crc32 = Crc32Algorithm.Append(crc32, valueSize);
            if (haveValue)
                crc32 = Crc32Algorithm.Append(crc32, value.Slice.Buffer, value.Slice.Offset, value.Slice.Length);

            writer.WriteInt32((int)crc32);
            writer.WriteRaw(headerBytes);
            writer.WriteRaw(keySize);
            if (haveKey)
                writer.WriteRaw(key.Slice);
            writer.WriteRaw(valueSize);
            if (haveValue)
                writer.WriteRaw(value.Slice);
        }

        public const int FixedEntrySize = 8 /* Offset */ + 4 /* Message Size */;
        public const int FixedMessageSize = 4 /* CRC */ + 4 /* Key.Length */ + 4 /* Value.Length */;

        public static int CalcHeaderSize(Message message) => message.Version == 0 ? 2 : 10;

        public static int CalcMessageSize(Message message)
        {
            var key = message.Key;
            var value = message.Value;
            return FixedMessageSize + CalcHeaderSize(message) + (key?.Length ?? -1) + (value?.Length ?? -1);
        }

        public static int CalcEntrySize(Message message)
        {
            return CalcMessageSize(message) + FixedEntrySize;
        }
    }
}
