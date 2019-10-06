package com.alibaba.streamcompute.wdl;

import com.alibaba.streamcompute.impl.TikvServiceImpl;
import com.alibaba.streamcompute.service.TiKVStorageService;
import com.alibaba.streamcompute.tools.Constants;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

public class Predict {

  //  private StorageService storageService = new HBaseServiceImpl();
  //  private static final String PD_ADDRESS = "127.0.0.1:2379";
  private static TiKVStorageService storageService = new TikvServiceImpl(Constants.PD_ADDRESS);

  public Row generateSample(String userId, List<String> itemIds) throws IOException, Exception {
    return storageService.generateSample(userId, itemIds);
  }

  public static Tuple2<String, List<String>> getTopK(Row row) {
    Tuple2<String, List<String>> res = new Tuple2<>();
    List<String> topk = new ArrayList<>();
    String valueStr = String.valueOf(row.getField(0)).trim();
    valueStr = valueStr.substring(0, valueStr.length() - 1);
    String[] keys = valueStr.split(",");
    String userId = keys[0];
    String[] items = keys[1].split("\\|");
    List<Tuple2<String, Float>> tuple2List = new ArrayList<>();
    for (String item : items) {
      if (!StringUtils.isBlank(item)) {
        String[] itemKeys = item.split("-");
        Tuple2<String, Float> tuple = new Tuple2<>();
        tuple.setField(itemKeys[0], 0);
        tuple.setField(Float.valueOf(itemKeys[1]), 1);
        tuple2List.add(tuple);
      }
    }
    tuple2List.sort((o1, o2) -> (o1.f1 - o2.f1 > 0 ? -1 : 1));
    for (int i = 0; i < 8; i++) {
      topk.add(tuple2List.get(i).f0);
    }
    res.setField(userId, 0);
    res.setField(topk, 1);
    return res;
  }
}
