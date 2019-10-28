package com.alibaba.streamcompute.data.tikv;

import com.alibaba.streamcompute.impl.TikvServiceImpl;
import com.alibaba.streamcompute.service.TiKVStorageService;
import com.alibaba.streamcompute.tools.Constants;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import org.tikv.kvproto.Kvrpcpb;

public class TiKVMain {
  public static void main(String[] args) throws Exception {
    SimpleDateFormat sdm = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    if (args[0].equals("itemput")) {
      String time_itemput1 = sdm.format(new Date());
      System.out.println("item put begins" + time_itemput1);

      CreateItemDataByTiKV.main(null);

      String time_itemput2 = sdm.format(new Date());
      System.out.println("item put ends" + time_itemput2);
    } else if (args[0].equals("userput")) {
      String time_userput1 = sdm.format(new Date());
      System.out.println("user put begins" + time_userput1);

      CreateUserDataByTiKV.main(null);

      String time_userput2 = sdm.format(new Date());
      System.out.println("user put ends" + time_userput2);
    } else if (args[0].equals("clickput")) {
      String time_clickput1 = sdm.format(new Date());
      System.out.println("click put begins" + time_clickput1);

      WriteClickRecordToTiKV.main(null);

      String time_clickput2 = sdm.format(new Date());
      System.out.println("click put ends" + time_clickput2);
    } else if (args[0].equals("scan")) {
      TiKVStorageService storageService = new TikvServiceImpl(Constants.PD_ADDRESS);
      String time_scan_user1 = sdm.format(new Date());
      System.out.println("user scan begins" + time_scan_user1);

      List<Kvrpcpb.KvPair> result =
          (List<Kvrpcpb.KvPair>) storageService.scanData("user", new ArrayList<>());

      String time_scan_user2 = sdm.format(new Date());
      System.out.println("user scan ends" + time_scan_user2);
      System.out.println(
          "---------------------------------------------user result.size:"
              + result.size()
              + "-------------------------------------");
      for (Kvrpcpb.KvPair user : result) {
        System.out.println(user.getKey().toStringUtf8() + '\t' + user.getValue().toStringUtf8());
      }

      String time_scan_item1 = sdm.format(new Date());
      System.out.println("item scan begins" + time_scan_item1);

      result = (List<Kvrpcpb.KvPair>) storageService.scanData("item", new ArrayList<>());

      String time_scan_item2 = sdm.format(new Date());
      System.out.println("item scan ends" + time_scan_item2);
      System.out.println(
          "---------------------------------------------item result.size:"
              + result.size()
              + "-------------------------------------");
      for (Kvrpcpb.KvPair item : result) {
        //        System.out.println(item.getKey().toStringUtf8() + '\t' +
        // item.getValue().toStringUtf8());
      }

      String time_scan_click1 = sdm.format(new Date());
      System.out.println("click scan begins" + time_scan_click1);

      result = (List<Kvrpcpb.KvPair>) storageService.scanData("click", new ArrayList<>());

      String time_scan_click2 = sdm.format(new Date());
      System.out.println("click scan ends" + time_scan_click2);
      System.out.println(
          "---------------------------------------------click result.size:"
              + result.size()
              + "-------------------------------------");
      for (Kvrpcpb.KvPair click : result) {
        System.out.println(click.getKey().toStringUtf8() + '\t' + click.getValue().toStringUtf8());
      }
    } else if (args[0].equals("deleteAll")) {
      TiKVStorageService storageService = new TikvServiceImpl(Constants.PD_ADDRESS);
      storageService.deleteDataByTableName("user");
      storageService.deleteDataByTableName("item");
      storageService.deleteDataByTableName("click");
      storageService.deleteIndexByTableName("user");
      storageService.deleteIndexByTableName("item");
      storageService.deleteIndexByTableName("click");
    } else if (args[0].equals("getids")) {
      TiKVStorageService storageService = new TikvServiceImpl(Constants.PD_ADDRESS);
      Set<String> itemIds = storageService.getItemIds();
      System.out.println(
          "---------------------------------------------itemIds.size:"
              + itemIds.size()
              + "-------------------------------------");
      for (String item : itemIds) System.out.println(item);
    }
  }
}
