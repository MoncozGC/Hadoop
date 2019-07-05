package com.JadePenG.kafka;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.*;

/**
 * @author Peng
 */
public class TestConsumer {
    private static kafka.javaapi.consumer.ConsumerConnector consumer;

    public static void main(String[] args) {
        Properties props = new Properties();
        // zookeeper 配置
        props.put("zookeeper.connect", "bigdata111:2181");
        // group 代表一个消费组
        props.put("group.id", "day070101");
        // zk连接超时
        props.put("zookeeper.session.timeout.ms", "4000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        /**
         *  kafka0.10版本配置
         * 从哪里开始消费数据[latest, earliest, none]
         *  earliest：当各个分区下有已递交的offset时，从递交的offset开始消费，无递交的offset时，从头开始消费
         *  latest：当各个分区下有已递交的offset时，从递交的offset开始消费，无递交的offset时，消费新产生的该分区下的数据
         *  none：当个分区都存在已递交的offset时，从递交的offset开始消费，只要有一个分区不存在已递交的offset，则抛出异常
         */
        //kafka0.8版本
        props.put("auto.offset.reset", "smallest");
        // 序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");

        ConsumerConfig config = new ConsumerConfig(props);

        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put("test", new Integer(1));

        StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
        StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());

        Map<String, List<KafkaStream<String, String>>> consumerMap = consumer.createMessageStreams(topicCountMap, keyDecoder, valueDecoder);
        KafkaStream<String, String> stream = consumerMap.get("test").get(0);
        ConsumerIterator<String, String> it = stream.iterator();
        while (it.hasNext()) {
            System.out.println(it.next());
        }
    }
}