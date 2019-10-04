package com.alibaba.streamcompute.impl;

import static com.alibaba.streamcompute.tools.HBaseUtil.getFilterInfo;
import static com.alibaba.streamcompute.tools.HBaseUtil.getRow;
import static com.alibaba.streamcompute.tools.Util.getSample;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.streamcompute.service.StorageService;
import com.alibaba.streamcompute.tools.HBaseUtil;
import com.alibaba.streamcompute.tools.Util;
import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;
import org.apache.flink.types.Row;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseServiceImpl implements StorageService, Serializable {

  @Override
  public List<Result> scanData(String tableName, ArrayList<Map<String, String>> filterInfos)
      throws IOException {
    Connection connection = HBaseUtil.getHbaseConnection();
    HTable table = (HTable) connection.getTable(TableName.valueOf(tableName));
    FilterList filterList = new FilterList();
    for (Map<String, String> filterInfo : filterInfos) {
      SingleColumnValueFilter filter =
          new SingleColumnValueFilter(
              Bytes.toBytes(filterInfo.get("family")),
              Bytes.toBytes(filterInfo.get("filterKey")),
              CompareFilter.CompareOp.EQUAL,
              Bytes.toBytes(filterInfo.get("filterValue")));
      filter.setFilterIfMissing(true);
      filterList.addFilter(filter);
    }
    Scan scan = new Scan();
    scan.setFilter(filterList);
    Iterator<Result> results = table.getScanner(scan).iterator();
    List<Result> list = new ArrayList<>();
    while (results.hasNext()) {
      list.add(results.next());
    }
    connection.close();
    return list;
  }

  @Override
  public List<Cell> getDataByRowKey(String tableName, String rowKey) throws IOException {
    Connection connection = HBaseUtil.getHbaseConnection();
    Get get = new Get(rowKey.getBytes());
    HTable table = (HTable) connection.getTable(TableName.valueOf(tableName));
    Result result = table.get(get);
    List<Cell> cells = result.listCells();
    connection.close();
    return cells;
  }

  @Override
  public void writeData(String tableName, String cfName, String rowKey, String data)
      throws IOException {
    Connection connection = HBaseUtil.getHbaseConnection();
    HTable table = (HTable) connection.getTable(TableName.valueOf(tableName));

    Put put = new Put(Bytes.toBytes(rowKey));
    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(cfName), Bytes.toBytes(data));
    table.put(put);
    connection.close();
  }

  @Override
  public void updateI2i(
      Object result,
      Map<String, Map<String, Integer>> userRecord,
      Map<String, Map<String, Integer>> i2i) {
    List<Result> list = (List<Result>) result;
    try {
      for (Result result1 : list) {
        String value = "";
        for (Cell cell : result1.listCells()) {
          value =
              Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength())
                  + "\t"
                  + value;
          System.out.println(value);
        }
        Util.updateUserRecordMap(value, userRecord);
      }
    } catch (Exception ignore) {
      System.out.println("读click数据失败：数据不存在或者hbase读取失败！");
    }
    Util.createI2i(userRecord, i2i);
  }

  @Override
  public Set<String> getItemIds() throws IOException {
    List<Result> results = scanData("item", new ArrayList<>());

    Set<String> ids = new HashSet<>();
    try {
      for (Result result : results) {
        for (Cell cell : result.listCells()) {
          String name =
              Bytes.toString(
                  cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
          String value =
              Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
          if ("item_id".equals(name.trim())) { // trim方法是去除首位的空字符
            ids.add(value.trim());
          }
        }
      }
    } catch (Exception ignore) {
    }
    return ids;
  }

  @Override
  public Map<String, Integer> getUserClickRecord(String userId) throws IOException {
    Map<String, Map<String, Integer>> userRecord = new HashMap<>();
    ArrayList<Map<String, String>> userFilterInfos = new ArrayList<>();
    userFilterInfos.add(getFilterInfo("user_id", userId, "cf"));
    List<Result> results = scanData("click", userFilterInfos);
    try {
      for (Result result : results) {
        String value = "";
        for (Cell cell : result.listCells()) {
          value =
              Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength())
                  + "\t"
                  + value;
        }
        Util.updateUserRecordMap(value, userRecord);
      }
    } catch (Exception ignore) {
      System.out.println("no history data for user: " + userId);
      return new HashMap<>();
    }
    return userRecord.get(userId);
  }

  @Override
  public Map<String, Map<String, Object>> getI2i() throws IOException {
    Map<String, Map<String, Object>> i2i = new HashMap<>();
    long time = System.currentTimeMillis();
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
    String date = df.format(time);
    try {

      List<Cell> cells = getDataByRowKey("i2i", date);
      String value = "";

      for (Cell cell : cells) {
        value =
            Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength())
                + "\t"
                + value;
      }
      Map<String, Object> i2iJSONMap = JSONObject.parseObject(value);
      for (String key : i2iJSONMap.keySet()) {
        Map<String, Object> itemMap = JSONObject.parseObject(i2iJSONMap.get(key).toString());
        i2i.put(key, itemMap);
      }

    } catch (Exception e) {
      System.out.println("table i2i for date " + date + "is not exist, please update!");
    }
    return i2i;
  }

  @Override
  public org.apache.flink.types.Row generateSample(String userId, List<String> itemIds)
      throws IOException {

    ArrayList<Map<String, String>> userFilterInfos = new ArrayList<>();
    userFilterInfos.add(getFilterInfo("user_id", userId, "cf"));
    Result userRow = scanData("user", userFilterInfos).get(0);
    Map<String, String> userInfo = getRow(userRow);

    StringBuilder result = new StringBuilder();
    for (String itemId : itemIds) {

      ArrayList<Map<String, String>> itemFilterInfos = new ArrayList<>();
      itemFilterInfos.add(getFilterInfo("item_id", itemId, "cf"));
      Result itemRow = scanData("item", itemFilterInfos).get(0);
      Map<String, String> itemInfo = getRow(itemRow);

      Map<String, String> sample = getSample(userInfo, itemInfo);
      result.append(JSONObject.toJSONString(sample));
      result.append(",");
    }
    org.apache.flink.types.Row row = new Row(1);
    row.setField(0, result.toString().substring(0, result.length() - 1));
    return row;
  }
}
