package com.JadePenG.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author Peng
 */
public class TestProducer {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "bigdata111:9092,bigdata112:9092,bigdata113:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 100000; i++) {
            ProducerRecord<String, String> km = new ProducerRecord<String, String>("test", "this is a msg=====" + i);
            System.out.println(km);
            producer.send(km);
            Thread.sleep(1000);
        }
    }
}