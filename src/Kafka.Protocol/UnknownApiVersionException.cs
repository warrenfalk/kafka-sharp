using System;

namespace Kafka.Protocol
{
    public class UnknownApiVersionException : Exception
    {
        public ApiKey ApiKey { get; }
        public short ApiVersion { get; }

        public UnknownApiVersionException(short apiVersion, ApiKey apiKey)
            : base($"Unknown version {apiVersion} for api {apiKey.ToString()}")
        {
            ApiVersion = apiVersion;
            ApiKey = apiKey;
        }
    }
}