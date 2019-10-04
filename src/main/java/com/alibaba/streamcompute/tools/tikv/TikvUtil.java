package com.alibaba.streamcompute.tools.tikv;

import java.util.HashMap;
import java.util.Map;
import shade.com.google.protobuf.ByteString;

public class TikvUtil {

  public static Map<String, String> getRow(ByteString tvalue) {
    Map<String, String> row = new HashMap<>();
    String val = tvalue.toStringUtf8();
    String[] col_value = val.split(",");
    String[] cv;
    for (String s : col_value) {
      cv = s.split(":");
      String name = cv[0];
      String value = cv[1];
      row.put(name, value);
    }
    return row;
  }

  public static Map<String, String> getFilterInfo(
      String filter_Key, String filter_Value, String family) {
    Map<String, String> filterInfo = new HashMap<>();
    filterInfo.put("filterKey", filter_Key);
    filterInfo.put("filterValue", filter_Value);
    filterInfo.put("family", family);
    return filterInfo;
  }
}
