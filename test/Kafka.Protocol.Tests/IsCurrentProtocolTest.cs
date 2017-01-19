using System.IO;
using System.Net.Http;
using System.Reflection;
using System.Text;
using Xunit;

namespace Kafka.Protocol.Tests
{
    /// <summary>
    /// The purpose of this test is to check that the protocol.html file that details the kafka protocol
    /// has not changed on the kafka site since the version of assembly being tested
    /// 
    /// So it is generally alright if this test is failing, but will raise awareness to the fact that the
    /// code is behind.
    /// 
    /// In order for this to work, we must never commit a change to the protocol.html file that does not
    /// also accompany support in the code for that change.
    /// 
    /// </summary>
    public class IsCurrentProtocolTest
    {
        [Fact]
        public void IsCurrentProtocol()
        {
            var assembly = typeof(Kafka.Protocol.ApiKey).GetTypeInfo().Assembly;
            var stream = assembly.GetManifestResourceStream("Kafka.Protocol.embeddbg.protocol.html");
            if (stream == null) // if testing the release version, this won't exist, so just assume we're good
                return;
            var reader = new StreamReader(stream, Encoding.UTF8);
            var client = new HttpClient();
            var current = client.GetStringAsync("http://kafka.apache.org/protocol.html").Result;
            var built = reader.ReadToEnd();
            Assert.Equal(current, built);
        }
    }
}
