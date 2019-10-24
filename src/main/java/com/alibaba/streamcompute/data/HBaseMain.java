package com.alibaba.streamcompute.data;

import com.alibaba.streamcompute.impl.HBaseServiceImpl;
import com.alibaba.streamcompute.service.StorageService;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.hadoop.hbase.client.*;

public class HBaseMain {
  public static void main(String[] args) throws Exception {
    SimpleDateFormat sdm = new SimpleDateFormat("yyyy-mm-dd");
    if (args[0].equals("itemput")) {
      String time_itemput1 = sdm.format(new Date());
      System.out.println("item put begins: " + time_itemput1);

      CreateItemData.main(null);

      String time_itemput2 = sdm.format(new Date());
      System.out.println("item put ends" + time_itemput2);
    } else if (args[0].equals("userput")) {
      String time_userput1 = sdm.format(new Date());
      System.out.println("user put begins: " + time_userput1);

      CreateUserData.main(null);

      String time_userput2 = sdm.format(new Date());
      System.out.println("user put ends" + time_userput2);
    } else if (args[0].equals("clickput")) {
      String time_clickput1 = sdm.format(new Date());
      System.out.println("click put begins: " + time_clickput1);

      WriteClickRecordToHbase.main(null);

      String time_clickput2 = sdm.format(new Date());
      System.out.println("click put ends" + time_clickput2);
    } else if (args[0].equals("scan")) {
      String time_scan1 = sdm.format(new Date());
      System.out.println("scan begins: " + time_scan1);

      StorageService storageService = new HBaseServiceImpl();
      List<Result> results = (List<Result>) storageService.scanData("item", new ArrayList<>());
      System.out.println("item.size is : " + results.size());

      results = (List<Result>) storageService.scanData("user", new ArrayList<>());
      System.out.println("user.size is : " + results.size());

      results = (List<Result>) storageService.scanData("click", new ArrayList<>());
      System.out.println("click.size is : " + results.size());

      String time_scan2 = sdm.format(new Date());
      System.out.println("scan ends" + time_scan2);
    }
  }
}
