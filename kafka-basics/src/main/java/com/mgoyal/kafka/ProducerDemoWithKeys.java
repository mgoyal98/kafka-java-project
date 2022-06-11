package com.mgoyal.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Kafka Producer!");

        // Create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


        for (int i = 0; i < 10; i++) {

            String topic = "demo_java";
            String value = "hello_world " + i;
            String key = "id_" + i;

            // Create a producer records
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

            // Send data - async
            producer.send(producerRecord, (metadata, e) -> {
                // executes everytime a record is successfully sent or an exception thrown
                if (e == null) {
                    log.info("\n\n===============\n" +
                            "Received new metadata \n" +
                            "Topic:" + metadata.topic() + "\n" +
                            "Key:" + producerRecord.key() + "\n" +
                            "Partition: " + metadata.partition() + "\n" +
                            "Offset: " + metadata.offset() + "\n" +
                            "Timestamp: " + metadata.timestamp() +
                            "\n==================");
                    return;
                }
                log.error("Error while producing ", e);
            });
        }

        // Flush data - sync
        producer.flush();

        // Flush and close
        producer.close();

    }
}
