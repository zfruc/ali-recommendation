package com.alibaba.streamcompute.i2i;

import static com.alibaba.streamcompute.tools.HBaseUtil.getFilterInfo;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.streamcompute.impl.HBaseServiceImpl;
import com.alibaba.streamcompute.service.StorageService;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class UpdateI2i {
  private static StorageService storageService = new HBaseServiceImpl();

  public static void main(String[] args) throws IOException {

    Map<String, Map<String, Integer>> userRecord = new HashMap<>();
    Map<String, Map<String, Integer>> i2i = new HashMap<>();

    ArrayList<Map<String, String>> userFilterInfos = new ArrayList<>();
    userFilterInfos.add(getFilterInfo("flag", "1", "cf"));
    Object result = storageService.scanData("click", userFilterInfos);

    storageService.updateI2i(result, userRecord, i2i);

    String json = JSONObject.toJSONString(i2i);
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
    long time = System.currentTimeMillis();
    String date = df.format(time);

    storageService.writeData("i2i", "i2i", date, json);
  }
}
