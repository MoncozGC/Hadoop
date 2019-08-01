package com.JadePenG.step04;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

/**
 * 单词统计写入到kafka(wordCount)
 *
 * @author Peng
 */
public class GenerateWords {
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "node01:9092,node02:9092,node03:9092");
        //key和value的序列化方式
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //发送数据的时候是否需要应答
        //1：leader节点做出应答
        //0：leader节点不做应答
        //-1,all: follower->leader->producer
        props.setProperty("acks", "1");

        KafkaProducer producer = new KafkaProducer<String, String>(props);

        while (true) {
            Thread.sleep(500);

            //uuid作为key
            String key = UUID.randomUUID().toString();
            int base = 97;
            //遍历26个字母
            int code = new Random().nextInt(26) + base;
            char word = (char) code;
            //生产者数据  创建要发送到Kafka的记录
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("0702", key, String.valueOf(word));
            //发送数据
            producer.send(record);
            System.out.println(record);
        }
    }
}
