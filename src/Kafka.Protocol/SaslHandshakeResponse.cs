using System;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Protocol
{
    public interface SaslHandshakeResponse
    {
        int Version { get; }
        short ErrorCode { get; }
        IEnumerable<string> EnabledMechanisms { get; }
    }

    class SaslHandshakeResponseImpl : SaslHandshakeResponse
    {
        public int Version { get; }
        public short ErrorCode { get; }
        public IEnumerable<string> EnabledMechanisms { get; }

        public SaslHandshakeResponseImpl(
            int version,
            short errorCode,
            IEnumerable<string> enabledMechanisms)
        {
            Version = version;
            ErrorCode = errorCode;
            EnabledMechanisms = enabledMechanisms;
        }

        public static DecoderVersions<SaslHandshakeResponse> Versions = new DecoderVersions<SaslHandshakeResponse>(
            ApiKey.SaslHandshake,
            reader => new SaslHandshakeResponseImpl(
                version: 0,
                errorCode: reader.ReadInt16(),
                enabledMechanisms: reader.ReadList(r => r.ReadString())
            )
        );

        public override string ToString() => string.Join(Environment.NewLine, EnabledMechanisms.Select(v => v.ToString()));
    }

}
