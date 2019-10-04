package com.alibaba.streamcompute.tools;

public class Constants {

  public static final String KAFKA_SOURCE_TOPIC = "userclick";
  public static final String KAFKA_BOOTSTRAP = "bootstrap.servers";
  public static final String KAFKA_BOOTSTRAP_VALUE = "localhost:9092";
  public static final String KAFKA_BATCH_SIZE = "batch.size";
  public static final String KAFKA_BUFFER_MEMORY = "buffer.memory";
  public static final String KAFKA_KEY_SERIALIZER = "key.serializer";
  public static final String KAFKA_VALUE_SERIALIZER = "value.serializer";
  public static final String KAFKA_RETRIES = "retries";
  public static final String KAFKA_LINGER_MS = "linger.ms";
  public static final String KAFKA_ACKS = "acks";
  public static final String ZK_CONNECT = "zookeeper.connect";
  public static final String ZOOKEEPER_QUORUM_VALUE = "localhost";
  public static final String ZOOKEEPER_CLIENT_PORT_VALUE = "2181";
  public static final String MODEL_CKPT = "model.ckpt-1384";
  //  public static final String PD_ADDRESS = "202.112.113.25:2379";
  public static final String PD_ADDRESS = "127.0.0.1:2379";
  public static final int ROWID_MAX = Integer.MAX_VALUE;
}
