package org.example;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ProducerDemo {

    private static final Logger logger = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
    public static void main(String[] args) {
        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // properties.setProperty("batch.size", String.valueOf(400));

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        List<ProducerRecord<String, String>> listOfRecords = new ArrayList<>(30);
        for(int i = 0; i < 10; i++) {
            for(int j = 0; j < 30; j++) {
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world: " + i + ", " + j);
                listOfRecords.add(producerRecord);
                producer.send(producerRecord, (recordMetadata, e) -> {
                    if(e == null) {
                        logger.info("Received new metadata \nTopic: {}\nPartition: {}\nOffset: {}\nTimestamp: {}", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing");
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        producer.flush();

        producer.close();
    }
}