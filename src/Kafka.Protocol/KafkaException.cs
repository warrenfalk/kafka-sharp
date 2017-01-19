using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Kafka.Protocol
{
    public class KafkaException : Exception
    {
        private KafkaError KafkaError { get; }

        public KafkaException(KafkaError kafkaError)
            : base(kafkaError.Message)
        {
            this.KafkaError = kafkaError;
        }
    }
}
