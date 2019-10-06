package com.alibaba.streamcompute.service;

import com.google.protobuf.ByteString;

import java.io.IOException;
import java.util.*;

public interface TiKVStorageService {
  Object scanData(String tableName, ArrayList<Map<String, String>> filters) throws Exception;

  // 处理train_data和i2i表的插入
  int writeDataWithJSON(String tableName, String data) throws IOException;
  // 处理item和user表的插入
  int writeData(String tableName, Map<String, String> data) throws Exception;

  boolean createUniqueIndex(String tableName, String rowkey_name, String rowkey_value, int rowid);

  boolean createNoUniqueIndex(String tableName, String rowkey_name, String rowkey_value, int rowid, String user_id);

  void updateI2i(
      Object result,
      Map<String, Map<String, Integer>> userRecord,
      Map<String, Map<String, Integer>> i2i);

  String getDataByUniqueIndexKey(String tableName, String indexKey, String indexValue);

  String getDataByNoUniqueIndexKey(String tableName, String indexKey, String indexValue, int rowID);

  Map<String, Map<String, Object>> getI2i() throws IOException;

  Set<String> getItemIds() throws Exception;

  Map<String, Integer> getUserClickRecord(String userId) throws IOException, Exception;

  org.apache.flink.types.Row generateSample(String userId, List<String> itemIds)
      throws Exception;

  void createTiKVClient();

  void delete(Object key);

  void deleteDataByTableName(String tableName) throws Exception;

  void deleteIndexByTableName(String tableName);
}
