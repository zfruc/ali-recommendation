package com.alibaba.streamcompute.data;

import static com.alibaba.streamcompute.tools.HBaseUtil.*;
import static com.alibaba.streamcompute.tools.Util.getSample;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.streamcompute.impl.HBaseServiceImpl;
import com.alibaba.streamcompute.service.StorageService;
import com.alibaba.streamcompute.tools.HBaseUtil;
import java.io.*;
import java.util.*;
import org.apache.hadoop.hbase.client.*;
import scala.collection.mutable.StringBuilder;

// 生成训练数据 数据存储在train_data表里面
public class GenerateTrainData {

  private static BufferedWriter bw;
  private static StorageService storageService;

  public static void main(String[] args) throws IOException {
    // 该文件在哪？
    File file = new File("/Users/zhangpeng/venv/work/train.data");
    FileWriter fw = new FileWriter(file);
    bw = new BufferedWriter(fw);
    storageService = new HBaseServiceImpl(); // hbase服务的接口

    Connection connection = HBaseUtil.getHbaseConnection();
    List<Result> results =
        (List<Result>) storageService.scanData("click", new ArrayList<>()); // 获取click表的结果

    try {

      for (Result result : results) {
        // row中应保存了Hbase的某一个rowkey行所包含的所有列的值，其中每一个元素都是由<列名，列值>组成的
        Map<String, String> row = getRow(result);

        if (!row.containsKey("date")) { // 确保每个rowkey行都有date值
          row.put("date", "2019-09-05");
        }

        String itemId = row.get("item_id");
        String userId = row.get("user_id");
        // userFilterInfos只用到了一个Map<String, String>，保存的是在进行scanData时的过滤条件，即<filterKey,user_id>,
        // <filterValue,userId>,<family,cf>
        ArrayList<Map<String, String>> userFilterInfos = new ArrayList<>();
        userFilterInfos.add(getFilterInfo("user_id", userId, "cf"));

        ArrayList<Map<String, String>> itemFilterInfos = new ArrayList<>();
        itemFilterInfos.add(getFilterInfo("item_id", itemId, "cf"));

        // 因为user_id是唯一的，所以通过get(0)得到第一个Result就是保存的user_id所对应的行数据，然后通过getRow函数把Result类型
        // 转换成Map<String,String>，
        Map<String, String> userInfo =
            getRow(((List<Result>) storageService.scanData("user", userFilterInfos)).get(0));
        Map<String, String> itemInfo =
            getRow(((List<Result>) storageService.scanData("item", itemFilterInfos)).get(0));

        // getSample是把userInfo和itemInfo中所有的<K,V>对合并
        Map<String, String> sample = getSample(userInfo, itemInfo);
        sample.put("label", row.get("flag"));
        sample.put("date", row.get("date"));

        long time = System.currentTimeMillis();
        // feature_string返回Json字符串，并且过滤了value值为null的KV对
        String feature_string = JSONObject.toJSONString(sample);
        String rowkey = userId + "-" + itemId + "-" + time;
        storageService.writeData(
            "train_data", "com/alibaba/streamcompute/data", rowkey, feature_string);

        StringBuilder keys = new StringBuilder();
        for (String key : sample.keySet()) {
          keys.append("\"");
          keys.append(key);
          keys.append("\",");
        }
        System.out.println(keys);
      }
      bw.flush();
      fw.close();
      bw.close();
      connection.close();
    } catch (Exception ignore) {
    }
  }
  /*
      public static void writeToCSV(Map<String, String> data) throws IOException {
          String value = "";
          for (Map.Entry entry : data.entrySet()) {
              value = value + entry.getValue() + ",";
          }
          String result = value.substring(0, value.length() - 1) + "\r\n";
          bw.write(result);
      }

  */
}
