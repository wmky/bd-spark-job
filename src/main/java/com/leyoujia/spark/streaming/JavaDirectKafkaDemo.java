package com.leyoujia.spark.streaming;

import java.util.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.*;

import org.apache.spark.streaming.Durations;

public class JavaDirectKafkaDemo {

    public static void main(String[] args) throws Exception {
/*        if (args.length < 3) {
            System.err.println("Usage: JavaDirectKafkaWordCount <brokers> <topics>\n" +
                    "  <brokers> is a list of one or more Kafka brokers\n" +
                    "  <topics> is a list of one or more kafka topics to consume from\n" +
                    "  <group.id> is consumer name\n\n");
            System.exit(1);
        }

        String brokers = args[0];
        String topics = args[1];
        String groupId = args[2];*/

        String brokers = "172.16.18.2:9092,172.16.5.29:9092,172.16.5.30:9092";
        String topics = "test-wechat-event";
        String groupId = "local_debug_spark_job1716";

        // Create context with a 2 seconds batch interval
        SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount").setMaster("local[4]");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));

        Collection<String> topicsCollect = Arrays.asList(topics.split(","));
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", groupId);
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);

        // Create direct kafka stream with brokers and topics
        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topicsCollect, kafkaParams)
                );
        stream.map(s -> s.value()).print(10);
        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }
}
