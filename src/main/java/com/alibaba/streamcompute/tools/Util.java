package com.alibaba.streamcompute.tools;

import com.alibaba.flink.ml.cluster.MLConfig;
import com.alibaba.flink.ml.cluster.role.AMRole;
import com.alibaba.flink.ml.cluster.role.PsRole;
import com.alibaba.flink.ml.cluster.role.WorkerRole;
import com.alibaba.flink.ml.operator.client.FlinkJobHelper;
import java.util.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;

public class Util {

  public static Properties getKafkaProPerties() {
    Properties properties = new Properties();
    properties.setProperty(Constants.KAFKA_BOOTSTRAP, Constants.KAFKA_BOOTSTRAP_VALUE);
    properties.setProperty(
        Constants.ZK_CONNECT,
        Constants.ZOOKEEPER_QUORUM_VALUE + "," + Constants.ZOOKEEPER_CLIENT_PORT_VALUE);
    return properties;
  }

  public static void updateUserRecordMap(
      String line, Map<String, Map<String, Integer>> userRecord) {
    if (!StringUtils.isBlank(line)) {

      String[] cells = line.trim().split("\t");
      String userId = cells[0];
      String itemId = cells[1];
      String flag = cells[2];

      Map<String, Integer> itemMap;

      if (!userRecord.containsKey(userId)) {
        itemMap = new HashMap<>();
        userRecord.put(userId, itemMap);
      } else {
        itemMap = userRecord.get(userId);
      }

      int num = 0;
      if (itemMap.containsKey(itemId)) {
        num = itemMap.get(itemId);
      }

      if (flag.equals("1")) {
        itemMap.put(itemId, num + 2);
      } else {
        itemMap.put(itemId, num - 1);
      }
    }
  }

  public static void createI2i(
      Map<String, Map<String, Integer>> userRecord, Map<String, Map<String, Integer>> i2i) {

    for (Map<String, Integer> record : userRecord.values()) {
      List<String> list = new ArrayList<>(record.keySet());
      for (int i = 0; i < list.size(); i++) {
        String key1 = list.get(i);
        for (int j = i; j < list.size(); j++) {

          int num1 = 1;
          int num2 = 1;
          String key2 = list.get(j);

          Map<String, Integer> temp1 = new HashMap<>();
          Map<String, Integer> temp2 = new HashMap<>();
          if (i2i.containsKey(key1)) {
            temp1 = i2i.get(key1);
          }
          if (i2i.containsKey(key2)) {
            temp2 = i2i.get(key2);
          }
          if (temp1.containsKey(key2)) {
            num1 = temp1.get(key2) + 1;
          }
          if (temp2.containsKey(key1)) {
            num2 = temp1.get(key1) + 1;
          }
          temp1.put(key2, num1);
          temp2.put(key1, num2);
          i2i.put(key1, temp1);
          i2i.put(key2, temp2);
        }
      }
    }
  }

  public static Map<String, String> getSample(
      Map<String, String> userInfo, Map<String, String> itemInfo) {
    Map<String, String> sample = new HashMap<>();
    sample.putAll(userInfo);
    sample.putAll(itemInfo);
    return sample;
  }

  public static void execTableJobCustom(MLConfig mlConfig, StreamExecutionEnvironment streamEnv) {
    FlinkJobHelper helper = new FlinkJobHelper();
    helper.like(
        new WorkerRole().name(), mlConfig.getRoleParallelismMap().get(new WorkerRole().name()));
    helper.like(new PsRole().name(), mlConfig.getRoleParallelismMap().get(new PsRole().name()));
    helper.like(new AMRole().name(), 1);
    helper.like("SourceConversion", 1);
    helper.like("SinkConversion", 1);
    helper.like("debug_source", 1);
    helper.like("SINK", 1);
    StreamGraph streamGraph = helper.matchStreamGraph(streamEnv.getStreamGraph());
    String plan = FlinkJobHelper.streamPlan(streamGraph);
    System.out.println(plan);
    //        streamEnv.execute();
  }
}
