package com.alibaba.streamcompute.i2i.tikv;

import static com.alibaba.streamcompute.tools.tikv.TikvUtil.getFilterInfo;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.streamcompute.impl.TikvServiceImpl;
import com.alibaba.streamcompute.service.TiKVStorageService;
import com.alibaba.streamcompute.tools.Constants;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.tikv.kvproto.Kvrpcpb;

public class UpdateI2iByTiKV {

  public static void main(String[] args) throws IOException, Exception {
    //    String PD_ADDRESS = "127.0.0.1:2379";
    TiKVStorageService storageService = new TikvServiceImpl(Constants.PD_ADDRESS); // 创建Tikv服务

    Map<String, Map<String, Integer>> userRecord = new HashMap<>();
    Map<String, Map<String, Integer>> i2i = new HashMap<>();

    ArrayList<Map<String, String>> userFilterInfos = new ArrayList<>();
    userFilterInfos.add(getFilterInfo("flag", "1", "cf"));
    List<Kvrpcpb.KvPair> result =
        (List<Kvrpcpb.KvPair>) storageService.scanData("click", userFilterInfos);

    storageService.updateI2i(result, userRecord, i2i);

    String json = JSONObject.toJSONString(i2i);
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
    long time = System.currentTimeMillis();
    String date = df.format(time);

    // storageService.writeDataWithJSON("i2i", "i2i", date, json);
    // 创建i2i表数据
    int rowid = storageService.writeDataWithJSON("i2i", json);
    // 创建i2i表基于date的unique索引
    boolean isIndexCreated = storageService.createUniqueIndex("i2i", "date", date, rowid);
    if (isIndexCreated) {
      System.out.println("i2i表成功创建基于date列的索引！！");
    }
  }
}
