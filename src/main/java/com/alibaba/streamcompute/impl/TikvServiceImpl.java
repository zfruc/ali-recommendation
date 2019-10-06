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

import com.sun.jna.platform.win32.OaIdl;
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
          throws Exception{
    if(Constants.INDEX_ON){
      return _scanData_PlanA(tableName,filters);
    }
    else
      return _scanData_PlanB(tableName,filters);
  }

  public List<Kvrpcpb.KvPair> _scanData_PlanA(String tableName, ArrayList<Map<String, String>> filters)
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
              String.format("%s#%s#%s", tableName + ",", "r" + ",", String.valueOf(1)));
      ByteString tkey_max =
          ByteString.copyFromUtf8(
              String.format(
                  "%s#%s#%s", tableName + ",", "r" + ",", Constants.ROWID_MAX));
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
                      "%s#%s#%s#%s", tableName + ",", "i" + ",", filterKey + ",", filterValue));
          ByteString index = client.get(ikey);
          ByteString targetKey =
              ByteString.copyFromUtf8(
                  String.format("%s#%s#%s", tableName + ",", "r" + ",", index.toStringUtf8()));
          tmpResult = client.scan(targetKey, 1);
        }
        // else scan click by user_id or flag
        else if (tableName.equals("click")) {
          ByteString ikey_min =
              ByteString.copyFromUtf8(
                  String.format(
                      "%s#%s#%s#%s#%s",
                      tableName + ",",
                      "i" + ",",
                      filterKey + ",",
                      filterValue + ",",
                      String.valueOf(0)));

          ByteString ikey_max =
              ByteString.copyFromUtf8(
                  String.format(
                      "%s#%s#%s#%s#%s",
                      tableName + ",",
                      "i" + ",",
                      filterKey + ",",
                      filterValue + ",",
                      String.valueOf(Constants.ROWID_MAX)));

          List<Kvrpcpb.KvPair> indexList = client.scan(ikey_min, ikey_max);
          for (Kvrpcpb.KvPair index : indexList) {
            List<Kvrpcpb.KvPair> scanResult =
                client.scan(
                    ByteString.copyFromUtf8(
                        String.format(
                            "%s#%s#%s",
                            tableName + ",", "r" + ",", index.getValue().toStringUtf8())),
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
    session.close();

    return result;
  }

  public List<Kvrpcpb.KvPair> _scanData_PlanB(String tableName, ArrayList<Map<String, String>> filters)
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
    // the key format of "click" is different from "user" & "item"
    if (filters.isEmpty()) {
      if(tableName.equals("user") || tableName.equals("item")){
        ByteString tkey_min =
                ByteString.copyFromUtf8(
                        String.format("%s#%s#%s", tableName + ",", "r" + ",", String.valueOf(0)));
        ByteString tkey_max =
                ByteString.copyFromUtf8(
                        String.format(
                                "%s#%s#%s", tableName + ",", "r" + ",", Constants.USERID_MAX));
//        System.out.println(tkey_min.toStringUtf8() + '\t' + tkey_max.toStringUtf8());
        result = client.scan(tkey_min, tkey_max);
      }
      else{
        ByteString tkey_min =
                ByteString.copyFromUtf8(
                        String.format("%s#%s#%s#%s", tableName + ",", "r" + ",", 0 + ",", 0));
        ByteString tkey_max =
                ByteString.copyFromUtf8(
                        String.format("%s#%s#%s#%s", tableName + ",", "r" + ",", Constants.USERID_MAX + ",", Constants.ROWID_MAX));
        result = client.scan(tkey_min, tkey_max);
      }
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
          ByteString targetKey =
                  ByteString.copyFromUtf8(
                          String.format("%s#%s#%s", tableName + ",", "r" + ",", filterInfo.get("filterValue")));
          tmpResult = client.scan(targetKey, 1);
        }
        // else scan click by user_id or flag
        else if (tableName.equals("click")) {
          if(filterInfo.get("filterKey").equals("user_id")){
            ByteString tkey_min =
                    ByteString.copyFromUtf8(
                            String.format("%s#%s#%s#%s", tableName + ",", "r" + ",", filterInfo.get("filterValue") + ",", String.valueOf(0)));
            ByteString tkey_max =
                    ByteString.copyFromUtf8(
                            String.format("%s#%s#%s#%s", tableName + ",", "r" + ",", filterInfo.get("filterValue") + ",", Constants.ROWID_MAX));
            tmpResult = client.scan(tkey_min,tkey_max);
          }
          else if(filterInfo.get("filterKey").equals("flag")){
            ByteString ikey_min =
                    ByteString.copyFromUtf8(
                            String.format(
                                    "%s#%s#%s#%s#%s",
                                    tableName + ",",
                                    "i" + ",",
                                    filterKey + ",",
                                    filterValue + ",",
                                    String.valueOf(0)));

            ByteString ikey_max =
                    ByteString.copyFromUtf8(
                            String.format(
                                    "%s#%s#%s#%s#%s",
                                    tableName + ",",
                                    "i" + ",",
                                    filterKey + ",",
                                    filterValue + ",",
                                    String.valueOf(Constants.ROWID_MAX)));
            List<Kvrpcpb.KvPair> indexList = client.scan(ikey_min, ikey_max);
            for (Kvrpcpb.KvPair index : indexList) {
              List<Kvrpcpb.KvPair> scanResult =
                      client.scan(
                              ByteString.copyFromUtf8(
                                      String.format("%s#%s#%s", tableName + ",", "r" + ",", index.getValue())),
                              1);
              tmpResult.addAll(scanResult);
            }
          }
          else
            throw new Exception(String.format("target table %s don't support scan", tableName));
          }
        
        // If this is the first filter, tmpResult should be put into result
        // else tmpResult should intersect with result
        if (!isInited) result.addAll(tmpResult);
        else result.retainAll(tmpResult);
      }
    }
    session.close();

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
    // 注意，因为tableName含有"_"，所以采用了","来作分隔符
    // tkey由 tablePrefix_rowPrefix_rowID 构成
    ByteString tkey =
        ByteString.copyFromUtf8(
            String.format("%s#%s#%s", tableName + ",", "r" + ",", String.valueOf(RowID)));
    RowID++; // 每插入一行数据RowID自增1
    ByteString tvalue = ByteString.copyFromUtf8(data);
    client.put(tkey, tvalue);
    // 关闭数据库连接
    session.close();
    return RowID;
  }

  @Override
  // 处理item、user和click表的插入
  public int writeData(String tableName, Map<String, String> data) throws Exception {
    if(Constants.INDEX_ON)
      return _writeData_PlanA(tableName,data);
    else
      return _writeData_PlanB(tableName,data);
  }



  public int _writeData_PlanA(String tableName, Map<String, String> data){
    // 建立数据库连接
    TiSession session = TiSession.create(TiConfiguration.createRawDefault(PD_ADDRESS));
    if (client == null) {
      client = session.createRawClient();
    }
    // 创建表数据（根据HBaseServiceImpl里的调用，发现只有train_data表和i2i表用到了writeData方法来创建HTable）
    // 注意，因为tableName含有"_"，所以采用了","来作分隔符
    // former version use ";" as separator, now changing to ","
    // because ";" in ascii is bigger than "9", it will confuse key range split
    // if ";" was used as separator, #0:#1 < #10:#1 < #11:#1 < #19:#1 < #1:#1 < #20:#1 < #2:#1
    // tkey由 tablePrefix_rowPrefix_rowID 构成
    ByteString tkey =
            ByteString.copyFromUtf8(
                    String.format("%s#%s#%s", tableName + ",", "r" + ",", String.valueOf(RowID)));
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


  public int _writeData_PlanB(String tableName, Map<String, String> data) throws Exception {
    // 建立数据库连接
    TiSession session = TiSession.create(TiConfiguration.createRawDefault(PD_ADDRESS));
    if (client == null) {
      client = session.createRawClient();
    }

    // for "user" table, the tkey is tablePrefix_rowPrefix_userID
    // for "item" table, the tkey is tablePrefix_rowPrefix_itemID
    // for "click" table, the tkey is tablePrefix_rowPrefix_userID_rowID

    ByteString tkey;
    if(tableName.equals("user")){
      tkey = ByteString.copyFromUtf8(
              String.format("%s#%s#%s", tableName + ",", "r" + ",", data.get("user_id")));
    }
    else if (tableName.equals("item")){
      tkey = ByteString.copyFromUtf8(
              String.format("%s#%s#%s", tableName + ",", "r" + ",", data.get("item_id")));
    }
    else if (tableName.equals("click")){
      tkey = ByteString.copyFromUtf8(
              String.format("%s#%s#%s#%s", tableName + ",", "r" + ",", data.get("user_id") + ",", String.valueOf(RowID)));
    }
    else {
      throw new Exception("passing a wrong table to writeData function: " + tableName);
    }
    RowID++;

    // tvalue由"col1Name:col1value,col2Name:col2value"组成
    String valueBuilder = "";
    int kv_nums = data.size() - 1;
    int i = 0;
    for (Map.Entry<String, String> entry : data.entrySet()) {
      valueBuilder = valueBuilder + entry.getKey() + ":" + entry.getValue();
      if (i < kv_nums) {
        valueBuilder += ":";
      }
    }
    ByteString tvalue = ByteString.copyFromUtf8(valueBuilder);

    client.put(tkey, tvalue);
    System.out.println(tkey.toStringUtf8() + '\t' + tvalue.toStringUtf8());
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
                "%s#%s#%s#%s", tableName + ",", "i" + ",", index_name + ",", index_value));
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
          String tableName, String index_name, String index_value, int rowid, String user_id) {
    if(Constants.INDEX_ON)
      return _createNoUniqueIndex_PlanA(tableName,index_name,index_value,rowid, user_id);
    else
      return _createNoUniqueIndex_PlanB(tableName,index_name,index_value,rowid, user_id);
  }

  public boolean _createNoUniqueIndex_PlanA(
      String tableName, String index_name, String index_value, int rowid, String user_id) {
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
                tableName + ",",
                "i" + ",",
                index_name + ",",
                index_value + ",",
                String.valueOf(rowid)));
    // ivalue为RowID
    ByteString ivalue = ByteString.copyFromUtf8(String.valueOf(RowID));
    client.put(ikey, ivalue);
    session.close();
    if (client.get(ikey).equals(ivalue)) {
      return true;
    } else return false;
  }

  public boolean _createNoUniqueIndex_PlanB(
          String tableName, String index_name, String index_value, int rowid, String user_id){
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
                            tableName + ",",
                            "i" + ",",
                            index_name + ",",
                            index_value + ",",
                            String.valueOf(rowid)));
    // ivalue为 user_id:rowID
    ByteString ivalue = ByteString.copyFromUtf8(
            String.format(
                    "%s#%s",
                    user_id + ",",
                    String.valueOf(rowid)));
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
            String.format("%s#%s#%s#%s", tableName + ",", "i" + ",", indexKey + ",", indexValue));
    ByteString ivalue = client.get(ikey);
    String rowid = ivalue.toStringUtf8();
    ByteString tkey =
        ByteString.copyFromUtf8(String.format("%s#%s#%s", tableName + ",", "r" + ",", rowid));
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
                tableName + ",",
                "i" + ",",
                indexKey + ",",
                indexValue + ",",
                String.valueOf(rowID)));
    ByteString ivalue = client.get(ikey);
    if (ivalue == null) return null;
    String rowid = ivalue.toStringUtf8();
    ByteString tkey =
        ByteString.copyFromUtf8(String.format("%s#%s#%s", tableName + ",", "r" + ",", rowid));
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
  public Set<String> getItemIds() throws Exception {

    List<Kvrpcpb.KvPair> results = scanData("item", new ArrayList<>());
    System.out.println("------------item.size()"+ results.size() + "----------------");

    Set<String> ids = new HashSet<>();
    String[] cells;
    String[] value;
    try {
      for (Kvrpcpb.KvPair result : results) {
        cells = result.getValue().toStringUtf8().split(",");
        for(String cell: cells){
          if (cell.contains("item_id")){
            value = cell.split(":");
            System.out.println("value" + value[0]);
            if ("item_id".equals(value[0].trim())) { // trim方法是去除首位的空字符
              ids.add(value[1].trim());
              break;
            }
          }
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

  @Override
  public void createTiKVClient(){
    TiConfiguration conf = TiConfiguration.createRawDefault(PD_ADDRESS);
    TiSession session = TiSession.create(conf);
    if (client == null) {
      client = session.createRawClient();
    }
    session.close();
  }

  @Override
  public void delete(Object key) {
    TiConfiguration conf = TiConfiguration.createRawDefault(PD_ADDRESS);
    TiSession session = TiSession.create(conf);
    if (client == null) {
      client = session.createRawClient();
    }
    client.delete((ByteString) key);
    session.close();
  }

  @Override
  public void deleteDataByTableName(String tableName) throws Exception {

    List<Kvrpcpb.KvPair> results = scanData(tableName,new ArrayList<>());
    for (Kvrpcpb.KvPair result : results){
      delete(result.getKey());
    }
  }

  @Override
  public void deleteIndexByTableName(String tableName) {
    TiConfiguration conf = TiConfiguration.createRawDefault(PD_ADDRESS);
    TiSession session = TiSession.create(conf);
    if (client == null) {
      client = session.createRawClient();
    }

    switch (tableName){
      // for "user" "item", if Constants.INDEX_ON, the index column is user_id/item_id, else index doesn't exist
      case "user":
      case "item":
        if (Constants.INDEX_ON){
          ByteString ikey_min =
                  ByteString.copyFromUtf8(
                          String.format(
                                  "%s#%s#%s#%s", tableName + ",", "i" + ",", tableName +"_id" + ",", "0"));
          //the index key is String.format("%s#%s#%s#%s", tableName + ",", "i" + ",", indexColumnName + ",", indexColumnValue)
          ByteString ikey_max =
                  ByteString.copyFromUtf8(
                          String.format(
                                  "%s#%s#%s#%s", tableName + ",", "i" + ",", tableName +"_id" + ",", Constants.USERID_MAX));
          List<Kvrpcpb.KvPair> indexResults = client.scan(ikey_min,ikey_max);
          for (Kvrpcpb.KvPair indexResult: indexResults) {
            client.delete(indexResult.getKey());
          }
        }
        break;
      case "click":
        //for "click", if Constants.INDEX_ON, there is a unique index on column user_id and a non-unique index on column flag
        // if Constants.INDEX_ON is false, there is only a non-unique index on column flag.
        // Pay attention, although the value format of non-unique index is different under these two cases, the key format is same.
        if (Constants.INDEX_ON){
          ByteString ikey_min =
                  ByteString.copyFromUtf8(
                          String.format(
                                  "%s#%s#%s#%s", tableName + ",", "i" + ",", "user_id" + ",", "0"));
          //the index key is String.format("%s#%s#%s#%s", tableName + ",", "i" + ",", indexColumnName + ",", indexColumnValue)
          ByteString ikey_max =
                  ByteString.copyFromUtf8(
                          String.format(
                                  "%s#%s#%s#%s", tableName + ",", "i" + ",", "user_id" + ",", Constants.USERID_MAX));
          List<Kvrpcpb.KvPair> indexResults = client.scan(ikey_min,ikey_max);
          for (Kvrpcpb.KvPair indexResult: indexResults) {
            client.delete(indexResult.getKey());
          }
        }
        ByteString ikey_min = ByteString.copyFromUtf8(
                        String.format("%s#%s#%s#%s#%s",
                                tableName + ",",
                                "i" + ",",
                                "flag" + ",",
                                "0" + ",",
                                "0")); // the first "0" is flag, the second "0" is the minimize of rowid
        ByteString ikey_max = ByteString.copyFromUtf8(
                String.format("%s#%s#%s#%s#%s",
                        tableName + ",",
                        "i" + ",",
                        "flag" + ",",
                        "1" + ",",
                        Constants.ROWID_MAX)); // the "1" is flag
        // delete all data under non-unique index
        List<Kvrpcpb.KvPair> indexResults = client.scan(ikey_min,ikey_max);
        for (Kvrpcpb.KvPair indexResult: indexResults) {
          client.delete(indexResult.getKey());
        }
        break;
      default:
        System.out.println(String.format("---------------target table [%s] doesn't support deleteIndex--------------",tableName));
    }

    session.close();
  }


}
