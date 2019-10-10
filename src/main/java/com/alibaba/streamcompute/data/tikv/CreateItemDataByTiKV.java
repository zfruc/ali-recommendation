package com.alibaba.streamcompute.data.tikv;

import com.alibaba.streamcompute.impl.TikvServiceImpl;
import com.alibaba.streamcompute.service.TiKVStorageService;
import com.alibaba.streamcompute.tools.Constants;
import java.util.*;

public class CreateItemDataByTiKV {

  public static void main(String[] args) throws Exception {

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

    generate(200);
  }
  // 创建一个item表，该表是由一个"cf"列族，以及由15个特征作为列来形成的，generate方法会向该表插入200条记录
  public static void generate(int num) throws Exception {
    //    String PD_ADDRESS = "127.0.0.1:2379";
    TiKVStorageService storageService = new TikvServiceImpl(Constants.PD_ADDRESS); // 创建Tikv服务

    Random random = new Random();
    List<String> categories =
        Arrays.asList(
            "Women's clothing",
            "Men's",
            "Women's shoes",
            "makeups",
            "Watch",
            "Mobile phone",
            "Maternal baby",
            "toy",
            "Snack",
            "Tea wine",
            "Fresh",
            "fruit",
            "Home appliance",
            "Furniture",
            "Building materials",
            "car",
            "Accessories",
            "Home textile",
            "medicine",
            "Kitchenware",
            "Storage");

    List<String> cites =
        Arrays.asList(
            "beijing-beijing",
            "shanghai-shanghai",
            "guangzhou-guangdong",
            "shenzheng-shenzheng",
            "chengdu-sichuan",
            "hangzhou-zhejiang",
            "wuhan-hubei",
            "changsha-hunan",
            "kunming-yunnan",
            "xiamen-fujian",
            "zhengzhou-henan",
            "shijiazhuang-hebei");
    // 循环插入item表的KV数据
    String tableName = "item";
    int rowid;
    boolean isIndexCreated;
    for (int i = 0; i < num; i++) {
      // value:Map<String,String> data
      Map<String, String> itemFeature = new HashMap<>();

      itemFeature.put("item_id", String.valueOf(i));

      String item_expo_id = String.valueOf(random.nextInt(100));
      itemFeature.put("item_expo_id", item_expo_id);

      String item_category = categories.get(random.nextInt(categories.size()));
      itemFeature.put("item_category", item_category);

      String item_category_level1 = String.valueOf(random.nextInt(4));
      itemFeature.put("item_category_level1", item_category_level1);

      String city = cites.get(random.nextInt(cites.size()));
      itemFeature.put("item_seller_city", city.split("-")[0]);
      itemFeature.put("item_seller_prov", city.split("-")[1]);

      String item_purch_level = String.valueOf(random.nextInt(5));
      itemFeature.put("item_purch_level", item_purch_level);

      String item_gender = random.nextInt(2) < 1 ? "male" : "female";
      itemFeature.put("item_gender", item_gender);

      String item_buyer_age = String.valueOf(random.nextInt(8));
      itemFeature.put("item_buyer_age", item_buyer_age);

      String item_style_id = String.valueOf(random.nextInt(20));
      itemFeature.put("item_style_id", item_style_id);

      String item_material_id = String.valueOf(random.nextInt(20));
      itemFeature.put("item_material_id", item_material_id);

      String item_pay_class = String.valueOf(random.nextInt(5));
      itemFeature.put("item_pay_class", item_pay_class);

      String item_brand_id = String.valueOf(random.nextInt(100));
      itemFeature.put("item_brand_id", item_brand_id);

      String item__i_shop_id_ctr = String.valueOf(random.nextInt(100) / 10000.0);
      itemFeature.put("item__i_shop_id_ctr", item__i_shop_id_ctr);

      String item__i_brand_id_ctr = String.valueOf(random.nextInt(200) / 10000.0);
      itemFeature.put("item__i_brand_id_ctr", item__i_brand_id_ctr);

      String item__i_category_ctr = String.valueOf(random.nextInt(70) / 10000.0);
      itemFeature.put("item__i_category_ctr", item__i_category_ctr);
      // 调用writeData插入表数据
      rowid = storageService.writeData(tableName, itemFeature);
      // String tableName, String index_name, String index_value,int rowid
      if (Constants.INDEX_ON) {
        String index_name = "item_id";
        String index_value = String.valueOf(i);
        // 调用createUniqueIndex创建索引
        isIndexCreated =
            storageService.createUniqueIndex(tableName, index_name, index_value, rowid);
        if (isIndexCreated) {
          System.out.println("item表成功创建索引！！");
        }
      }
    }
  }
}
