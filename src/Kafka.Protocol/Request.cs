using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Kafka.Protocol
{
    public interface Request
    {
        ApiKey ApiKey { get; }
        short ApiVersion { get; }
        void WriteTo(ProtocolStreamWriter writer);
    }
}
