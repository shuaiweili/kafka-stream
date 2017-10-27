package com.git.lee.kafka.stream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * @author LISHUAIWEI
 * @date 2017/10/27 11:14
 */
public class WordCountLambdaExample {

    public static void main(String[] args) {
        final Properties streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-lambda-example");
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "10.100.51.198:9092");
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        final KStreamBuilder builder = new KStreamBuilder();
        final KStream<String, String> textLines = builder.stream(stringSerde, stringSerde, "word-count-input");
        final Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);

        final KTable<String, Long> wordCounts = textLines
                .flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
                .groupBy((key, word) -> {
                    System.out.printf("key:%s, word:%s\n", key, word);
                    return word;
                })
                .count("Counts");

        wordCounts.to(stringSerde, longSerde, "word-count-output");

        final KafkaStreams streams = new KafkaStreams(builder, streamsConfig);
        streams.cleanUp();
        streams.start();

        System.out.println("Topology: " + streams.toString());

        //producer send msg
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.100.51.198:9092");
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        System.out.println("start send msg...");
        Producer<String, String> producer = new KafkaProducer<>(producerConfig);
        producer.send(new ProducerRecord<>("word-count-input", "key", "all streams lead to kafka using kafka stream"));

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
