package com.github.schm1tz1.KafkaTransactionsExampleJava;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ProducerConsumerBuilder {

    final static String topicName = new String("transactions");

    static Consumer<String, String> getKafkaConsumer() throws IOException {
        Properties properties = new Properties();
        InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream("consumer.properties");
        properties.load(stream);
        return new KafkaConsumer<>(properties);
    }

    static Producer<String, String> getKafkaProducer() throws IOException {
        Properties properties = new Properties();
        InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream("producer.properties");
        properties.load(stream);
        return new KafkaProducer<>(properties);
    }

}