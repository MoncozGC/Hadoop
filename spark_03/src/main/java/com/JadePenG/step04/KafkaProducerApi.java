package com.JadePenG.step04;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 生产数据
 *
 * @author Peng
 */
public class KafkaProducerApi {
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();

        //连接kafka 9092
        properties.setProperty("bootstrap.servers", "192.168.25.111:9092, 192.168.25.112:9092, 192.168.25.113:9092");
        //设置序列化
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        /**
         *发送成功是否需要leader回应
         * 1: leader做出应答
         * 0: 未做应答
         * -1 all: follower-->leader-->produce  全部做出应答
         */
        properties.setProperty("acks", "1");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        int i = 1;
        while (i < 10000) {
            /**
             * partition: 如果指定分区就写入指定分区中
             * key: 没有指定分区编号就按照轮询的方式写入各个分区
             */
            ProducerRecord record = new ProducerRecord("0702", null, "" + i);
            System.out.println(record);
            //发送数据
            producer.send(record);
            Thread.sleep(100);
            i++;
        }

        producer.close();
        System.out.println("end");

    }
}
