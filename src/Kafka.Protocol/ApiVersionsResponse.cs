using System;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Protocol
{
    public interface ApiVersionsResponse
    {
        int Version { get; }
        KafkaError Error { get; }
        IEnumerable<ApiVersionSupport> ApiVersions { get; }
    }

    public interface ApiVersionSupport
    {
        short ApiKeyCode { get; }
        short MinVersion { get; }
        short MaxVersion { get; }
    }

    class ApiVersionsResponseImpl : ApiVersionsResponse
    {
        public int Version { get; }
        public KafkaError Error { get; }
        public IEnumerable<ApiVersionSupport> ApiVersions { get; }

        public ApiVersionsResponseImpl(
            int version,
            KafkaError error,
            IEnumerable<ApiVersionSupport> apiVersions)
        {
            Version = version;
            Error = error;
            ApiVersions = apiVersions;
        }

        public static DecoderVersions<ApiVersionsResponse> Decode = new DecoderVersions<ApiVersionsResponse>(
            ApiKey.ApiVersions,
            reader => new ApiVersionsResponseImpl(
                version: 0,
                error: reader.ReadErrorCode(),
                apiVersions: reader.ReadList(ApiVersionSupportImpl.Versions[0])
            )
        );

        public override string ToString() => string.Join(Environment.NewLine, ApiVersions.Select(v => v.ToString()));
    }

    class ApiVersionSupportImpl : ApiVersionSupport
    {
        public short ApiKeyCode { get; }
        public short MinVersion { get; }
        public short MaxVersion { get; }

        public ApiVersionSupportImpl(
            short apiKeyCode,
            short minVersion,
            short maxVersion)
        {
            ApiKeyCode = apiKeyCode;
            MinVersion = minVersion;
            MaxVersion = maxVersion;
        }

        public static DecoderVersions<ApiVersionSupport> Versions = new DecoderVersions<ApiVersionSupport>(
            ApiKey.None,
            reader => new ApiVersionSupportImpl(
                apiKeyCode: reader.ReadInt16(),
                minVersion: reader.ReadInt16(),
                maxVersion: reader.ReadInt16()
            )
        );

        public override string ToString() => $"{((ApiKey)ApiKeyCode).ToString()} {MinVersion} to {MaxVersion}";
    }

}
