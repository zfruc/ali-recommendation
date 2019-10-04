package com.alibaba.streamcompute.data;

import com.alibaba.streamcompute.tools.HBaseUtil;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Random;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

// 创建click表，rowkey是"time"，列族是"cf"，列包括"user_id"，"item_id"，"flag"和"date"，并往hbase写入3000条数据
public class WriteClickRecordToHbase {
  public static void main(String[] args) throws IOException {

    Connection connection = HBaseUtil.getHbaseConnection();
    HTable table = (HTable) connection.getTable(TableName.valueOf("click"));

    int recordNum = 3000;
    Random random = new Random();

    for (int i = 0; i < recordNum; i++) {
      long time = System.currentTimeMillis();
      SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
      String date = df.format(time);
      int userId = random.nextInt(50);
      int itemId = random.nextInt(200);
      int flag = random.nextInt(100) < 15 ? 1 : 0;
      String record = userId + "\t" + itemId + "\t" + flag + "\t\n";
      Put put = new Put(Bytes.toBytes(String.valueOf(time)));
      put.addColumn(
          Bytes.toBytes("cf"), Bytes.toBytes("user_id"), Bytes.toBytes(String.valueOf(userId)));
      put.addColumn(
          Bytes.toBytes("cf"), Bytes.toBytes("item_id"), Bytes.toBytes(String.valueOf(itemId)));
      put.addColumn(
          Bytes.toBytes("cf"), Bytes.toBytes("flag"), Bytes.toBytes(String.valueOf(flag)));
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("date"), Bytes.toBytes(date));
      table.put(put);
      System.out.println(record);
      //            Thread.sleep(1000);
    }
    connection.close();
  }
}
