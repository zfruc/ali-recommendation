package com.alibaba.streamcompute.impl;

import static com.alibaba.streamcompute.tools.HBaseUtil.getFilterInfo;
import static com.alibaba.streamcompute.tools.Util.getSample;
import static com.alibaba.streamcompute.tools.tikv.TikvUtil.getRow;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.streamcompute.service.TiKVStorageService;
import com.alibaba.streamcompute.tools.Constants;
import com.alibaba.streamcompute.tools.Util;
import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;
import org.apache.flink.types.Row;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.raw.RawKVClient;
import shade.com.google.protobuf.ByteString;

public class TikvServiceImpl implements TiKVStorageService, Serializable {
  private String PD_ADDRESS;
  private RawKVClient client;
  private int RowID; // 自增变量

  public TikvServiceImpl(String pd_addr) {
    PD_ADDRESS = pd_addr;
    RowID = 1;
  }
  // 注： 这里把Result换掉了，一个是因为它出现的地方不多，另一个是最关键的，就是Kvrpcpb.KvPair这种类型
  // 应该是RawKVClient底层在存储KV数据时用到的，所以我们最好直接用这种类型进行处理
  @Override
  public List<Kvrpcpb.KvPair> scanData(String tableName, ArrayList<Map<String, String>> filters)
      throws Exception {
    // create db connection
    TiSession session = TiSession.create(TiConfiguration.createRawDefault(PD_ADDRESS));
    if (client == null) {
      client = session.createRawClient();
    }

    // Iterator<Kvrpcpb.KvPair> iterator = rawScanIterator(conf, clientBuilder, startKey, endKey);
    List<Kvrpcpb.KvPair> result = new ArrayList<>();
    boolean isInited = false;
    // iterator.forEachRemaining(result::add);

    // If filters don't exist, just scan the whole specified table
    // Although the method "item", "user", "click" table written to DB was different from "i2i",
    // "train_data" table,
    // the key format is the same.
    if (filters.isEmpty()) {
      ByteString tkey_min =
          ByteString.copyFromUtf8(
              String.format("%s#%s#%s", tableName + ":", "r" + ":", String.valueOf(1)));
      ByteString tkey_max =
          ByteString.copyFromUtf8(
              String.format(
                  "%s#%s#%s", tableName + ":", "r" + ":", String.valueOf(Constants.ROWID_MAX)));
      result = client.scan(tkey_min, tkey_max);
    }
    // If filters exist, treat it separately
    else {
      // If there is more than one filter, it can't be handled now, throw an exception
      if (filters.size() != 1) throw new Exception("filter.size bigger than one !!!");
      for (Map<String, String> filterInfo : filters) {
        List<Kvrpcpb.KvPair> tmpResult = new ArrayList<>();
        String filterKey = filterInfo.get("filterKey");
        String filterValue = filterInfo.get("filterValue");

        // if we want to find user by user_id or find item by item_id
        if ((tableName.equals("user") && filterKey.equals("user_id"))
            || (tableName.equals("item") && filterKey.equals("item_id"))) {
          ByteString ikey =
              ByteString.copyFromUtf8(
                  String.format(
                      "%s#%s#%s#%s", tableName + ":", "i" + ":", filterKey + ":", filterValue));
          ByteString index = client.get(ikey);
          ByteString targetKey =
              ByteString.copyFromUtf8(
                  String.format("%s#%s#%s", tableName + ":", "r" + ":", index.toStringUtf8()));
          tmpResult = client.scan(targetKey, 1);
        }
        // else scan click by user_id or flag
        else if (tableName.equals("click")) {
          ByteString ikey_min =
              ByteString.copyFromUtf8(
                  String.format(
                      "%s#%s#%s#%s#%s",
                      tableName + ":",
                      "i" + ":",
                      filterKey + ":",
                      filterValue + ":",
                      String.valueOf(1)));

          ByteString ikey_max =
              ByteString.copyFromUtf8(
                  String.format(
                      "%s#%s#%s#%s#%s",
                      tableName + ":",
                      "i" + ":",
                      filterKey + ":",
                      filterValue + ":",
                      String.valueOf(Constants.ROWID_MAX)));

          List<Kvrpcpb.KvPair> indexList = client.scan(ikey_min, ikey_max);
          for (Kvrpcpb.KvPair index : indexList) {
            List<Kvrpcpb.KvPair> scanResult =
                client.scan(
                    ByteString.copyFromUtf8(
                        String.format(
                            "%s#%s#%s",
                            tableName + ":", "r" + ":", index.getValue().toStringUtf8())),
                    1);
            tmpResult.addAll(scanResult);
          }
        } else {
          throw new Exception(String.format("target table %s don't support scan", tableName));
        }

        // If this is the first filter, tmpResult should be put into result
        // else tmpResult should intersect with result
        if (!isInited) result.addAll(tmpResult);
        else result.retainAll(tmpResult);
      }
    }

    client.close();

    return result;
  }
  // 处理train_data和i2i表的插入，value数据类型用JSON来存，这样在代码中有两个地方存KV数据时比较方便
  @Override
  public int writeDataWithJSON(String tableName, String data) {
    // 建立数据库连接
    TiSession session = TiSession.create(TiConfiguration.createRawDefault(PD_ADDRESS));
    if (client == null) {
      client = session.createRawClient();
    }
    // 创建表数据（根据HBaseServiceImpl里的调用，发现只有train_data表和i2i表用到了writeData方法来创建HTable）
    // 注意，因为tableName含有"_"，所以采用了":"来作分隔符
    // tkey由 tablePrefix_rowPrefix_rowID 构成
    ByteString tkey =
        ByteString.copyFromUtf8(
            String.format("%s#%s#%s", tableName + ":", "r" + ":", String.valueOf(RowID)));
    RowID++; // 每插入一行数据RowID自增1
    ByteString tvalue = ByteString.copyFromUtf8(data);
    client.put(tkey, tvalue);
    // 关闭数据库连接
    session.close();
    return RowID;
  }

  @Override
  // 处理item、user和click表的插入
  public int writeData(String tableName, Map<String, String> data) {
    // 建立数据库连接
    TiSession session = TiSession.create(TiConfiguration.createRawDefault(PD_ADDRESS));
    if (client == null) {
      client = session.createRawClient();
    }
    // 创建表数据（根据HBaseServiceImpl里的调用，发现只有train_data表和i2i表用到了writeData方法来创建HTable）
    // 注意，因为tableName含有"_"，所以采用了":"来作分隔符
    // tkey由 tablePrefix_rowPrefix_rowID 构成
    ByteString tkey =
        ByteString.copyFromUtf8(
            String.format("%s#%s#%s", tableName + ":", "r" + ":", String.valueOf(RowID)));
    RowID++; // 每插入一行数据RowID自增1

    // tvalue由"col1Name:col1value,col2Name:col2value"组成
    String valueBuilder = "";
    int kv_nums = data.size() - 1;
    int i = 0;
    for (Map.Entry<String, String> entry : data.entrySet()) {
      valueBuilder = valueBuilder + entry.getKey() + ":" + entry.getValue();
      if (i < kv_nums) {
        valueBuilder += ",";
      }
    }
    ByteString tvalue = ByteString.copyFromUtf8(valueBuilder);
    client.put(tkey, tvalue);
    // 关闭数据库连接
    session.close();
    return RowID;
  }

  // 创建unique索引
  @Override
  public boolean createUniqueIndex(
      String tableName, String index_name, String index_value, int rowid) {
    // 建立数据库连接
    TiSession session = TiSession.create(TiConfiguration.createRawDefault(PD_ADDRESS));
    if (client == null) {
      client = session.createRawClient();
    }
    // ikey由tablePrefix_idxPrefix_indexID_indexColumnsValue 构成
    ByteString ikey =
        ByteString.copyFromUtf8(
            String.format(
                "%s#%s#%s#%s", tableName + ":", "i" + ":", index_name + ":", index_value));
    // ivalue为RowID
    ByteString ivalue = ByteString.copyFromUtf8(String.valueOf(rowid));
    client.put(ikey, ivalue);
    // 关闭数据库连接
    session.close();
    if (client.get(ikey).equals(ivalue)) {
      return true;
    } else return false;
  }
  // 创建非unique索引
  @Override
  public boolean createNoUniqueIndex(
      String tableName, String index_name, String index_value, int rowid) {
    // 建立数据库连接
    TiSession session = TiSession.create(TiConfiguration.createRawDefault(PD_ADDRESS));
    if (client == null) {
      client = session.createRawClient();
    }
    // ikey由tablePrefix_idxPrefix_indexID_ColumnsValue_rowID构成
    ByteString ikey =
        ByteString.copyFromUtf8(
            String.format(
                "%s#%s#%s#%s#%s",
                tableName + ":",
                "i" + ":",
                index_name + ":",
                index_value + ":",
                String.valueOf(rowid)));
    // ivalue为RowID
    ByteString ivalue = ByteString.copyFromUtf8(String.valueOf(RowID));
    client.put(ikey, ivalue);
    session.close();
    if (client.get(ikey).equals(ivalue)) {
      return true;
    } else return false;
  }

  @Override
  public void updateI2i(
      Object result,
      Map<String, Map<String, Integer>> userRecord,
      Map<String, Map<String, Integer>> i2i) {
    List<Kvrpcpb.KvPair> list = (List<Kvrpcpb.KvPair>) result;
    try {
      for (Kvrpcpb.KvPair result1 : list) {
        Map<String, String> row = getRow(result1.getValue());
        String value = "";
        for (Map.Entry<String, String> entry : row.entrySet()) {
          value = entry.getValue() + "\t" + value;
        }
        Util.updateUserRecordMap(value, userRecord);
      }
    } catch (Exception ignore) {
      System.out.println("读click数据失败：数据不存在或者hbase读取失败！");
    }
    Util.createI2i(userRecord, i2i);
  }
  // 根据唯一索引取值
  @Override
  public String getDataByUniqueIndexKey(String tableName, String indexKey, String indexValue) {
    // 建立数据库连接
    TiSession session = TiSession.create(TiConfiguration.createRawDefault(PD_ADDRESS));
    if (client == null) {
      client = session.createRawClient();
    }
    // 查找索引找到rowid
    ByteString ikey =
        ByteString.copyFromUtf8(
            String.format("%s#%s#%s#%s", tableName + ":", "i" + ":", indexKey + ":", indexValue));
    ByteString ivalue = client.get(ikey);
    String rowid = ivalue.toStringUtf8();
    ByteString tkey =
        ByteString.copyFromUtf8(String.format("%s#%s#%s", tableName + ":", "r" + ":", rowid));
    ByteString tvalue = client.get(tkey);
    String value = tvalue.toStringUtf8();
    // 关闭数据库连接
    session.close();
    return value;
  }
  // 根据非唯一索引取值
  @Override
  public String getDataByNoUniqueIndexKey(
      String tableName, String indexKey, String indexValue, int rowID) {
    // 建立数据库连接
    TiSession session = TiSession.create(TiConfiguration.createRawDefault(PD_ADDRESS));
    if (client == null) {
      client = session.createRawClient();
    }
    // 查找索引找到rowid
    ByteString ikey =
        ByteString.copyFromUtf8(
            String.format(
                "%s#%s#%s#%s#%s",
                tableName + ":",
                "i" + ":",
                indexKey + ":",
                indexValue + ":",
                String.valueOf(rowID)));
    ByteString ivalue = client.get(ikey);
    if (ivalue == null) return null;
    String rowid = ivalue.toStringUtf8();
    ByteString tkey =
        ByteString.copyFromUtf8(String.format("%s#%s#%s", tableName + ":", "r" + ":", rowid));
    ByteString tvalue = client.get(tkey);
    String value = tvalue.toStringUtf8();
    // 关闭数据库连接
    session.close();
    return value;
  }

  @Override
  public Map<String, Map<String, Object>> getI2i() throws IOException {
    Map<String, Map<String, Object>> i2i = new HashMap<>();
    long time = System.currentTimeMillis();
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
    String date = df.format(time);
    try {
      // 这里得到的tvalue应该就是i2i的json字符串
      String tvalue = getDataByUniqueIndexKey("i2i", "date", date);
      if (tvalue == null) return null;
      Map<String, Object> i2iJSONMap = JSONObject.parseObject(tvalue);
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
  public Set<String> getItemIds() throws IOException, Exception {
    List<Kvrpcpb.KvPair> results = scanData("item", new ArrayList<>());

    Set<String> ids = new HashSet<>();
    String[] cells;
    String[] value;
    try {
      for (Kvrpcpb.KvPair result : results) {
        cells = result.getValue().toStringUtf8().split(",");
        value = cells[0].split(":");
        if ("item_id".equals(value[0].trim())) { // trim方法是去除首位的空字符
          ids.add(value[1].trim());
        }
      }
    } catch (Exception ignore) {
    }
    return ids;
  }

  @Override
  public Map<String, Integer> getUserClickRecord(String userId) throws IOException, Exception {
    Map<String, Map<String, Integer>> userRecord = new HashMap<>();
    ArrayList<Map<String, String>> userFilterInfos = new ArrayList<>();
    userFilterInfos.add(getFilterInfo("user_id", userId, "cf"));
    List<Kvrpcpb.KvPair> results = scanData("click", userFilterInfos);
    try {
      for (Kvrpcpb.KvPair result : results) {
        Map<String, String> row = getRow(result.getValue());
        String value = "";
        for (Map.Entry<String, String> entry : row.entrySet()) {
          value = entry.getValue() + "\t" + value;
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
  public org.apache.flink.types.Row generateSample(String userId, List<String> itemIds)
      throws Exception {
    ArrayList<Map<String, String>> userFilterInfos = new ArrayList<>();
    userFilterInfos.add(getFilterInfo("user_id", userId, "cf"));
    Kvrpcpb.KvPair userRow = scanData("user", userFilterInfos).get(0);
    Map<String, String> userInfo = getRow(userRow.getValue());

    StringBuilder result = new StringBuilder();
    for (String itemId : itemIds) {

      ArrayList<Map<String, String>> itemFilterInfos = new ArrayList<>();
      itemFilterInfos.add(getFilterInfo("item_id", itemId, "cf"));
      Kvrpcpb.KvPair itemRow = scanData("item", itemFilterInfos).get(0);
      Map<String, String> itemInfo = getRow(itemRow.getValue());

      Map<String, String> sample = getSample(userInfo, itemInfo);
      result.append(JSONObject.toJSONString(sample));
      result.append(",");
    }
    org.apache.flink.types.Row row = new Row(1);
    row.setField(0, result.toString().substring(0, result.length() - 1));
    return row;
  }
}
