package com.alibaba.streamcompute.data.tikv;

import static com.alibaba.streamcompute.tools.Util.getSample;
import static com.alibaba.streamcompute.tools.tikv.TikvUtil.getFilterInfo;
import static com.alibaba.streamcompute.tools.tikv.TikvUtil.getRow;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.streamcompute.impl.TikvServiceImpl;
import com.alibaba.streamcompute.service.TiKVStorageService;
import com.alibaba.streamcompute.tools.Constants;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.tikv.kvproto.Kvrpcpb;

public class GenerateTrainDataByTiKV {
  // 已修改，只需看看getFilterInfo里面的三个参数在scanData中是否需要？不需要的可以删掉，另外能够满足scanData接口处的调用就可以了
  //  private static final String PD_ADDRESS = "127.0.0.1:2379";
  private static BufferedWriter bw;
  private static TiKVStorageService storageService;

  public static void main(String[] args) throws IOException, Exception {
    File file = new File("/tmp/train.data"); // 文件的内容似乎没有用到？
    FileWriter fw = new FileWriter(file);
    bw = new BufferedWriter(fw);
    storageService = new TikvServiceImpl(Constants.PD_ADDRESS); // 创建Tikv服务

    // 获取click表的结果
    List<Kvrpcpb.KvPair> results =
        (List<Kvrpcpb.KvPair>) storageService.scanData("click", new ArrayList<>());

    try {

      for (Kvrpcpb.KvPair result : results) {
        // row中应保存了Hbase的某一个rowkey行所包含的所有列的值，其中每一个元素都是由<列名，列值>组成的
        // 可以从result的value中解析到click表中的每一列的值，通过getRow方法把它们转换成Map<String, String>，这样就能按原来的方式进行处理
        Map<String, String> row = getRow(result.getValue());
        // ByteString tvalue = result.getValue();
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
            getRow(
                ((List<Kvrpcpb.KvPair>) storageService.scanData("user", userFilterInfos))
                    .get(0)
                    .getValue());
        Map<String, String> itemInfo =
            getRow(
                ((List<Kvrpcpb.KvPair>) storageService.scanData("item", itemFilterInfos))
                    .get(0)
                    .getValue());

        // getSample是把userInfo和itemInfo中所有的<K,V>对合并
        Map<String, String> sample = getSample(userInfo, itemInfo);
        sample.put("label", row.get("flag"));
        sample.put("date", row.get("date"));

        long time = System.currentTimeMillis();
        // feature_string返回Json字符串，并且过滤了value值为null的KV对
        String feature_string = JSONObject.toJSONString(sample);
        String index_name = "userId-itemId-time"; // 因为原来这个表的rowkey是由这三个组成的，所以这里直接把它们拼起来作索引
        String index_value = userId + "-" + itemId + "-" + time;
        int rowID = storageService.writeDataWithJSON("train_data", feature_string);
        storageService.createUniqueIndex("train_data", index_name, index_value, rowID);
      }
      bw.flush();
      fw.close();
      bw.close();
      // connection.close();
    } catch (Exception ignore) {
    }
  }
}
