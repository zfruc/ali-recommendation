package com.alibaba.streamcompute.i2i.tikv;

import com.alibaba.streamcompute.impl.TikvServiceImpl;
import com.alibaba.streamcompute.service.TiKVStorageService;
import com.alibaba.streamcompute.tools.Constants;
import java.io.IOException;
import java.util.*;
import org.apache.flink.api.java.tuple.Tuple2;

public class RecallFromTiKV {

  private Map<String, Map<String, Object>> i2i;
  //  String PD_ADDRESS = "127.0.0.1:2379";
  TiKVStorageService storageService = new TikvServiceImpl(Constants.PD_ADDRESS); // 创建Tikv服务

  private Map<String, Integer> getScore(Set<String> itemIdes, Map<String, Integer> userClickMap) {
    Map<String, Integer> result = new HashMap<>();
    for (String key : itemIdes) {
      if (i2i.containsKey(key)) {
        Map<String, Object> i2iItem = i2i.get(key);
        int score = compute(userClickMap, i2iItem);
        result.put(key, score);
      } else {
        result.put(key, 0);
      }
    }
    return result;
  }

  private Integer compute(Map<String, Integer> user, Map<String, Object> item) {
    int score = 0;
    for (String key : user.keySet()) {
      if (item.containsKey(key)) {
        score += (user.get(key) * Integer.valueOf(item.get(key).toString()));
      }
    }
    return score;
  }

  public Tuple2<String, List<String>> recall(String userId) throws IOException, Exception {
    i2i = storageService.getI2i();
    Set<String> itemIds = storageService.getItemIds();
    Map<String, Integer> userClickMap = storageService.getUserClickRecord(userId);
    Map<String, Integer> itemScores = getScore(itemIds, userClickMap);
    List<Map.Entry<String, Integer>> list = new ArrayList<>(itemScores.entrySet());
    list.sort((o1, o2) -> o2.getValue() - o1.getValue());
    int i = 0;

    List<String> recall = new ArrayList<>();
    for (Map.Entry<String, Integer> o1 : list) {
      recall.add(o1.getKey());
      i++;
      if (i == 50) {
        break;
      }
    }

    Tuple2<String, List<String>> tuple = new Tuple2<>();
    tuple.setField(userId, 0);
    tuple.setField(recall, 1);
    return tuple;
  }
}
