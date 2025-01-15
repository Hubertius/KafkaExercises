package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ProducerDemoKeys {

    private static final Logger logger = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
    public static void main(String[] args) {
        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // properties.setProperty("batch.size", String.valueOf(400));

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        List<ProducerRecord<String, String>> listOfRecords = new ArrayList<>(30);
        for (int i = 0; i < 2; i++) {
            for (int j = 0; j < 10; j++) {
                String producerName = "demo_java";
                String key = "id_" + j;
                String value = "Hello world: " + key;
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(producerName, key, value);
                listOfRecords.add(producerRecord);
                producer.send(producerRecord, (recordMetadata, e) -> {
                    if(e == null) {
                        logger.info("Received new metadata \nKey: {}\nTopic: {}\nPartition: {}\nOffset: {}\nTimestamp: {}", key, recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing");
                    }
                });
            }
        }

        producer.flush();

        producer.close();
    }
}
