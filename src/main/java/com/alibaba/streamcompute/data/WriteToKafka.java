package com.alibaba.streamcompute.data;

import com.alibaba.streamcompute.tools.Constants;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class WriteToKafka {

  public static void main(String[] args) {
    send("userclick");
  }

  private static void send(String topic) {

    Properties properties = new Properties();
    properties.put(Constants.KAFKA_BOOTSTRAP, Constants.KAFKA_BOOTSTRAP_VALUE);
    properties.put(Constants.KAFKA_BATCH_SIZE, 16384);
    properties.put(Constants.KAFKA_BUFFER_MEMORY, 33554432);
    properties.put(Constants.KAFKA_RETRIES, 0);
    properties.put(
        Constants.KAFKA_KEY_SERIALIZER, "org.apache.kafka.common.serialization.StringSerializer");
    properties.put(
        Constants.KAFKA_VALUE_SERIALIZER, "org.apache.kafka.common.serialization.StringSerializer");
    properties.put(Constants.KAFKA_LINGER_MS, 1);
    properties.put(Constants.KAFKA_ACKS, "all");

    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
    for (int i = 1; i < 10000; i++) {
      for (int j = 1; j < 50; j++) {
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        System.out.println(j);
        producer.send(new ProducerRecord<>(topic, String.valueOf(j)));
      }
    }
    producer.close();
  }
}
