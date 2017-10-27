package com.git.lee.kafka.stream;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author LISHUAIWEI
 * @date 2017/10/24 11:12
 */
public class WordCountExample {

    public static void main(String[] args) {
        Properties streamsConfiguration = new Properties();
        //Give the Streams application a unique name. The name must be unique in the kafka cluster
        //against which the application is run.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-example");
        //Where to find kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "10.100.51.198:9092");
        //Where to find the corresponding Zookeeper ensemble.
//        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "10.100.51.198:2181");
        //Specify default (de)serializers for record keys and record values
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // Set up serializers and deserializers, which we will use for overriding the default serdes
        // specified above.
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        // In the subsequent lines we define the processing topology of the Streams application
        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, String> textLines = builder.stream(stringSerde, stringSerde, "word-count-input");

        KStream<String, Long> wordCounts = textLines.flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split("\\W+")))
                .map((key, word) -> {
                    System.out.println("key: " + key + ", word: " + word);
                   return new KeyValue<>(word, word);
                })
                .groupByKey()
                .count("Counts")
                .toStream();

        wordCounts.to(stringSerde, longSerde, "word-count-example-out");

        //Now that we have finished the definition of the processing topology we can actually run
        //it via `start()`. The Streams application as a whole can be launched just like any
        //normal Java application that has a `main()` method.
        KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
        streams.start();
    }
}
