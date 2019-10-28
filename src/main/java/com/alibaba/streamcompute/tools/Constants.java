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
  // public static final String ZOOKEEPER_QUORUM_VALUE =
  // "192.168.1.101,192.168.1.104,192.168.1.105,192.168.1.106";
  public static final String ZOOKEEPER_QUORUM_VALUE = "11.227.70.150,11.251.155.243,11.251.155.142";
  public static final String ZOOKEEPER_CLIENT_PORT_VALUE = "2181";
  public static final String MODEL_CKPT = "model.ckpt-1384";
  public static final String PD_ADDRESS = "11.251.158.237:2379";
  //  public static final String PD_ADDRESS = "127.0.0.1:2379";
  public static final String ROWID_MAX =
      "9999999999"; // in ByteString, 0 < 10 < 11 < 19 < 1 < 20 < 20 < 29. so 9 is the biggest
  public static final String USERID_MAX = "9999999999"; // Integer.MAX_VALUE = 2147483647
  public static final boolean INDEX_ON = false;
  // If INDEX_ON is true, the key format of "user" "item" is tablePrefix_rowPrefix_rowID , and we
  // create a unique index on column user_id/item_id to get rowID
  // else if INDEX_ON is false, the key format of "user" "item" is
  // tablePrefix_rowPrefix_userId:value/itemID:value, and we won't create index.
  // If INDEX_ON is true, the key format of "click" is tablePrefix_rowPrefix_rowID, and we create a
  // unique index on column user_id , a non-unique index on column flag.
  // else if INDEX_ON is false, the key format of "click" is tablePrefix_rowPrefix_userID_rowID, and
  // we still create non-unique index on column flag, but the stored value
  // in index is userID_rowID.
}
