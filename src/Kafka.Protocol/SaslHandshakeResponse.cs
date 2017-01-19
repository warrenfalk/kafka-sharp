using System;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Protocol
{
    public interface SaslHandshakeResponse
    {
        int Version { get; }
        KafkaError Error { get; }
        IEnumerable<string> EnabledMechanisms { get; }
    }

    class SaslHandshakeResponseImpl : SaslHandshakeResponse
    {
        public int Version { get; }
        public KafkaError Error { get; }
        public IEnumerable<string> EnabledMechanisms { get; }

        public SaslHandshakeResponseImpl(
            int version,
            KafkaError error,
            IEnumerable<string> enabledMechanisms)
        {
            Version = version;
            Error = error;
            EnabledMechanisms = enabledMechanisms;
        }

        public static DecoderVersions<SaslHandshakeResponse> Versions = new DecoderVersions<SaslHandshakeResponse>(
            ApiKey.SaslHandshake,
            reader => new SaslHandshakeResponseImpl(
                version: 0,
                error: reader.ReadErrorCode(),
                enabledMechanisms: reader.ReadList(r => r.ReadString())
            )
        );

        public override string ToString() => string.Join(Environment.NewLine, EnabledMechanisms.Select(v => v.ToString()));
    }

}
