package com.github.schm1tz1.KafkaTransactionsExampleJava;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Random;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.github.schm1tz1.KafkaTransactionsExampleJava.ProducerConsumerBuilder.*;

class ProducerConsumerBuilderTest {

    @Test
    void testTransactions() {
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

    static void consumeTestTransaction() throws IOException {
        Consumer<String, String> consumer = getKafkaConsumer();
        consumer.subscribe(
                Collections.singleton(topicName)
        );
        ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(500));
        Stream<ConsumerRecord<String, String>> consumerRecordStream = StreamSupport.stream(consumerRecords.spliterator(), false);
        consumerRecordStream.forEach(stringStringConsumerRecord -> System.out.println(stringStringConsumerRecord.value()));
        consumer.commitSync();
    }

    static void produceTestTransaction(int identifierNumber, boolean abortTransaction) throws IOException {
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