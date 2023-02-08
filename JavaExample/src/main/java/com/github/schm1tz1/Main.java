package com.github.schm1tz1;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.Producer;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Main {

    final static String topicName = new String("transactiBons");

    private static Consumer<String, String> getKafkaConsumer() throws IOException {
        Properties properties = new Properties();
        InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream("consumer.properties");
        properties.load(stream);
        return new KafkaConsumer<>(properties);
    }

    private static Producer<String, String> getKafkaProducer() throws IOException {
        Properties properties = new Properties();
        InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream("producer.properties");
        properties.load(stream);
        return new KafkaProducer<>(properties);
    }


    public static void main(String[] args) {

        try {
            Random rand = new Random();
            int nextInt = rand.nextInt();

            produceTestTransaction(nextInt, false);
            produceTestTransaction(nextInt, true);
            consumeTestTransaction();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private static void consumeTestTransaction() throws IOException {
        Consumer<String, String> consumer = getKafkaConsumer();
        consumer.subscribe(
                Collections.singleton(topicName)
        );
        ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(500));
        Stream<ConsumerRecord<String, String>> consumerRecordStream = StreamSupport.stream(consumerRecords.spliterator(), false);
        consumerRecordStream.forEach(stringStringConsumerRecord -> System.out.println(stringStringConsumerRecord.value()));
        consumer.commitSync();
    }

    private static void produceTestTransaction(int identifierNumber, boolean abortTransaction) throws IOException {
        Producer<String, String> producer = getKafkaProducer();
        producer.initTransactions();

        producer.beginTransaction();
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, String.format("Transaction %d / %d (aborted: %b)", identifierNumber, i, abortTransaction));
            producer.send(record);
        }
        if(abortTransaction) {
            producer.abortTransaction();
        } else {
            producer.commitTransaction();
        }
    }
}