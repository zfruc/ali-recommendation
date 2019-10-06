package com.alibaba.streamcompute.data.tikv;

import com.alibaba.streamcompute.impl.TikvServiceImpl;
import com.alibaba.streamcompute.service.TiKVStorageService;
import com.alibaba.streamcompute.tools.Constants;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class WriteClickRecordToTiKV {
  public static void main(String[] args) throws Exception {
    //    String PD_ADDRESS = "127.0.0.1:2379";
    TiKVStorageService storageService = new TikvServiceImpl(Constants.PD_ADDRESS); // 创建Tikv服务
    int recordNum = 3000;
    Random random = new Random();

    // 循环插入click表的KV数据，同时暂时创建分别关于user_id和flag的非唯一索引
    String tableName = "click";
    int rowid;
    boolean isIndexCreated;
    for (int i = 0; i < recordNum; i++) {
      Map<String, String> clickFeature = new HashMap<>();

      long time = System.currentTimeMillis();
      SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
      String date = df.format(time);

      int userId = random.nextInt(50);
      int itemId = random.nextInt(200);
      int flag = random.nextInt(100) < 15 ? 1 : 0;
      clickFeature.put("user_id", String.valueOf(userId));
      clickFeature.put("item_id", String.valueOf(itemId));
      clickFeature.put("flag", String.valueOf(flag));
      clickFeature.put("date", date);
      // 调用writeData插入表数据
      rowid = storageService.writeData(tableName, clickFeature);
      if(Constants.INDEX_ON) {
        // 创建基于"user_id"的非Unique索引
        String userid_name = "user_id";
        String userid_value = String.valueOf(i);
        // 调用createNoUniqueIndex创建非unique索引
        isIndexCreated =
                storageService.createNoUniqueIndex(tableName, userid_name, userid_value, rowid, userid_value);
        if (isIndexCreated) {
//          System.out.println("成功创建基于user_id的非Unique索引！！");
        }
      }
      // 创建基于"flag"的非Unique索引
      String flag_name = "flag";
      String flag_value = String.valueOf(i);
      // 调用createNoUniqueIndex创建非unique索引
      isIndexCreated = storageService.createNoUniqueIndex(tableName, flag_name, flag_value, rowid, String.valueOf(i));
      if (isIndexCreated) {
//        System.out.println("成功创建基于flag的非Unique索引！！");
      }
      // System.out.println(record);
      //            Thread.sleep(1000);
    }
  }
}
