using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Kafka.Protocol
{
    public enum Compression : sbyte
    {
        None = 0,
        Gzip = 1,
        Snappy = 2,
    }
}
