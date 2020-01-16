package com.huag.kafka;


import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * kafka生产者
 * @author huag
 * @date 2019/12/15 13:35
 */
public class KafkaProducer {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());

        String topic = "testLog";


    }

}
