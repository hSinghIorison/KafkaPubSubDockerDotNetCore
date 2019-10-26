using System;
using System.Collections.Generic;
using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;

namespace KafkaPubSubDocker
{
    class Program
    {
        static void Main(string[] args)
        {
            string kafkaEndpoint = "127.0.0.1:9092";
            string kafkaTopic = "testtopic";

            var producerConfig = new Dictionary<string, object> {{"bootstrap.servers", kafkaEndpoint}};

            using var producer = new Producer<Null, string>(producerConfig, null, new StringSerializer(Encoding.UTF8));
            for (int i = 0; i < 10; i++)
            {
                var msg = $"Event {i}";
                var result = producer.ProduceAsync(kafkaTopic, null, msg).GetAwaiter().GetResult();
                Console.WriteLine($"Event {i} sent on partition {result.Partition} with offset {result.Offset}");
            }

            var consumerConfig  = new Dictionary<string, object>() 
            {
                {"groupId", "myconsumer"},
                {"bootstrap.servers", kafkaEndpoint }
            };

            using var consumer = new Consumer<Null, string>(consumerConfig, null, new StringDeserializer(Encoding.UTF8));
            consumer.OnMessage += Consumer_OnMessage;
            consumer.Subscribe(new List<string>{kafkaTopic});
            var cancelled = false;
            Console.CancelKeyPress += delegate(object o, ConsoleCancelEventArgs eventArgs)
            {
                eventArgs.Cancel = true;
                cancelled = true;
            };

            Console.WriteLine($"Ctrl-C to exit.");

            while (!cancelled)
            {
                consumer.Poll();
            }
        }

        private static void Consumer_OnMessage(object sender, Message<Null, string> e)
        {
            Console.WriteLine($"Received: {e.Value}");
        }
    }
}
