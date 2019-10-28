package com.alibaba.streamcompute.data;

import com.alibaba.streamcompute.impl.HBaseServiceImpl;
import com.alibaba.streamcompute.impl.TikvServiceImpl;
import com.alibaba.streamcompute.service.StorageService;
import com.alibaba.streamcompute.service.TiKVStorageService;
import com.alibaba.streamcompute.tools.Constants;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.hadoop.hbase.client.*;

public class HBaseMain {
  public static void main(String[] args) throws Exception {
    SimpleDateFormat sdm = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
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
      System.out.println("click put ends : " + time_clickput2);
    } else if (args[0].equals("scan")) {
      String time_scan1 = sdm.format(new Date());
      System.out.println("scan begins : " + time_scan1);

      StorageService storageService = new HBaseServiceImpl();
      List<Result> results = (List<Result>) storageService.scanData("item", new ArrayList<>());
      System.out.println("item.size is : " + results.size());

      results = (List<Result>) storageService.scanData("user", new ArrayList<>());
      System.out.println("user.size is : " + results.size());

      results = (List<Result>) storageService.scanData("click", new ArrayList<>());
      System.out.println("click.size is : " + results.size());

      String time_scan2 = sdm.format(new Date());
      System.out.println("scan ends : " + time_scan2);
    } else if (args[0].equals("load")) {
      TiKVStorageService storageService = new TikvServiceImpl(Constants.PD_ADDRESS);

      List<String> item_features = new ArrayList<>();
      item_features.add("item_id");
      item_features.add("item_expo_id");
      item_features.add("item_category");
      item_features.add("item_category_level1");
      item_features.add("item_seller_city");
      item_features.add("item_seller_prov");
      item_features.add("item_purch_level");
      item_features.add("item_gender");
      item_features.add("item_buyer_age");
      item_features.add("item_style_id");
      item_features.add("item_material_id");
      item_features.add("item_pay_class");
      item_features.add("item_brand_id");
      item_features.add("item__i_shop_id_ctr");
      item_features.add("item__i_brand_id_ctr");
      item_features.add("item__i_category_ctr");

      List<String> user_features = new ArrayList<>();
      user_features.add("user_id");
      user_features.add("pred_gender");
      user_features.add("pred_age_level");
      user_features.add("pred_career_type");
      user_features.add("pred_education_degree");
      user_features.add("pred_baby_age");
      user_features.add("pred_has_pet");
      user_features.add("pred_has_car");
      user_features.add("pred_life_stage");
      user_features.add("pred_has_house");
      user_features.add("os");

      List<String> click_features = new ArrayList<>();
      click_features.add("user_id");
      click_features.add("item_id");
      click_features.add("flag");
      click_features.add("date");

      switch (args[1]) {
        case "item":
          storageService.loadToTiKVFromHBase("item", item_features);
          break;
        case "user":
          storageService.loadToTiKVFromHBase("user", user_features);
          break;
        case "click":
          storageService.loadToTiKVFromHBase("click", click_features);
          break;
        default:
          System.out.println("table " + args[1] + " doesn't support loadToTiKVFromHBase now.");
      }
    }
  }
}
