package com.alibaba.streamcompute.service;

import java.io.IOException;
import java.util.*;

public interface TiKVStorageService {
  Object scanData(String tableName, ArrayList<Map<String, String>> filters) throws Exception;

  // 处理train_data和i2i表的插入
  int writeDataWithJSON(String tableName, String data) throws IOException;
  // 处理item和user表的插入
  int writeData(String tableName, Map<String, String> data) throws IOException;

  boolean createUniqueIndex(String tableName, String rowkey_name, String rowkey_value, int rowid);

  boolean createNoUniqueIndex(String tableName, String rowkey_name, String rowkey_value, int rowid);

  void updateI2i(
      Object result,
      Map<String, Map<String, Integer>> userRecord,
      Map<String, Map<String, Integer>> i2i);

  String getDataByUniqueIndexKey(String tableName, String indexKey, String indexValue);

  String getDataByNoUniqueIndexKey(String tableName, String indexKey, String indexValue, int rowID);

  Map<String, Map<String, Object>> getI2i() throws IOException;

  Set<String> getItemIds() throws IOException, Exception;

  Map<String, Integer> getUserClickRecord(String userId) throws IOException, Exception;

  public org.apache.flink.types.Row generateSample(String userId, List<String> itemIds)
      throws Exception;
}
