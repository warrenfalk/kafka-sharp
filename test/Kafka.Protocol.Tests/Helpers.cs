using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Kafka.Protocol.Tests
{
    public static class Helpers
    {
        public static byte[] FromHex(string hexString)
        {
            byte[] bytes = new byte[hexString.Length / 2];
            for (var i = 0; i < bytes.Length; i++)
            {
                var si = i << 1;
                bytes[i] = Convert.ToByte(hexString.Substring(si, 2), 16);
            }
            return bytes;
        }

        public static string ToHex(byte[] bytes)
        {
            return string.Join("", bytes.Select(b => b.ToString("x2")));
        }
    }
}
