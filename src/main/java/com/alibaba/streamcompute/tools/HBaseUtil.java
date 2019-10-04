package com.alibaba.streamcompute.tools;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseUtil {
  public static Configuration getHbaseConf() {
    Configuration hbaseConf = HBaseConfiguration.create();
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, Constants.ZOOKEEPER_QUORUM_VALUE);
    hbaseConf.set(HConstants.ZOOKEEPER_CLIENT_PORT, Constants.ZOOKEEPER_CLIENT_PORT_VALUE);
    hbaseConf.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000);
    hbaseConf.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000);
    return hbaseConf;
  }

  public static Connection getHbaseConnection() throws IOException {
    Configuration config = getHbaseConf();
    return ConnectionFactory.createConnection(config);
  }

  public static Map<String, String> getRow(Result result) {
    Map<String, String> row = new HashMap<>();
    for (Cell cell : result.listCells()) {
      String name =
          Bytes.toString(
              cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
      String value =
          Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
      row.put(name, value);
    }
    return row;
  }

  public static Map<String, String> getFilterInfo(
      String filterKey, String filterValue, String family) {
    Map<String, String> filterInfo = new HashMap<>();
    filterInfo.put("filterKey", filterKey);
    filterInfo.put("filterValue", filterValue);
    filterInfo.put("family", family);
    return filterInfo;
  }
}
