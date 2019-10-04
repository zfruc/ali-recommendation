package com.alibaba.streamcompute.service;

import java.io.IOException;
import java.util.*;
import org.apache.flink.types.Row;

public interface StorageService {
  Object scanData(String tableName, ArrayList<Map<String, String>> filters) throws IOException;

  Object getDataByRowKey(String tableName, String rowKey) throws IOException;

  void writeData(String tableName, String cfName, String rowKey, String data) throws IOException;

  void updateI2i(
      Object result,
      Map<String, Map<String, Integer>> userRecord,
      Map<String, Map<String, Integer>> i2i);

  Set<String> getItemIds() throws IOException;

  Map<String, Integer> getUserClickRecord(String userId) throws IOException;

  Map<String, Map<String, Object>> getI2i() throws IOException;

  Row generateSample(String userId, List<String> itemIds) throws IOException;
}
