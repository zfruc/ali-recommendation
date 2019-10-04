package com.alibaba.streamcompute.data;

import com.alibaba.streamcompute.tools.HBaseUtil;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

// 创建Item数据
public class CreateItemData {

  public static void main(String[] args) throws IOException {

    List<String> features = new ArrayList<>();
    features.add("item_id");
    features.add("item_expo_id");
    features.add("item_category");
    features.add("item_category_level1");
    features.add("item_seller_city");
    features.add("item_seller_prov");
    features.add("item_purch_level");
    features.add("item_gender");
    features.add("item_buyer_age");
    features.add("item_style_id");
    features.add("item_material_id");
    features.add("item_pay_class");
    features.add("item_brand_id");
    features.add("item__i_shop_id_ctr");
    features.add("item__i_brand_id_ctr");
    features.add("item__i_category_ctr");

    generate(200);
  }
  // 创建一个item表，该表是由一个"cf"列族，以及由15个特征作为列来形成的，generate方法会向该表插入200条记录
  public static void generate(int num) throws IOException {

    Connection connection = HBaseUtil.getHbaseConnection();
    HTable table = (HTable) connection.getTable(TableName.valueOf("item"));

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

    for (int i = 0; i < num; i++) {
      Put put = new Put(Bytes.toBytes(String.valueOf(i)));

      Map<String, String> itemFeature = new HashMap<>();

      itemFeature.put("item_id", String.valueOf(i));
      put.addColumn(
          Bytes.toBytes("cf"), Bytes.toBytes("item_id"), Bytes.toBytes(String.valueOf(i)));

      String item_expo_id = String.valueOf(random.nextInt(100));
      itemFeature.put("item_expo_id", item_expo_id);
      put.addColumn(
          Bytes.toBytes("cf"), Bytes.toBytes("item_expo_id"), Bytes.toBytes(item_expo_id));

      String item_category = categories.get(random.nextInt(categories.size()));
      itemFeature.put("item_category", item_category);
      put.addColumn(
          Bytes.toBytes("cf"), Bytes.toBytes("item_category"), Bytes.toBytes(item_category));

      String item_category_level1 = String.valueOf(random.nextInt(4));
      itemFeature.put("item_category_level1", item_category_level1);
      put.addColumn(
          Bytes.toBytes("cf"),
          Bytes.toBytes("item_category_level1"),
          Bytes.toBytes(item_category_level1));

      String city = cites.get(random.nextInt(cites.size()));
      itemFeature.put("item_seller_city", city.split("-")[0]);
      itemFeature.put("item_seller_prov", city.split("-")[1]);
      put.addColumn(
          Bytes.toBytes("cf"),
          Bytes.toBytes("item_seller_city"),
          Bytes.toBytes(city.split("-")[0]));
      put.addColumn(
          Bytes.toBytes("cf"),
          Bytes.toBytes("item_seller_prov"),
          Bytes.toBytes(city.split("-")[1]));

      String item_purch_level = String.valueOf(random.nextInt(5));
      itemFeature.put("item_purch_level", item_purch_level);
      put.addColumn(
          Bytes.toBytes("cf"), Bytes.toBytes("item_purch_level"), Bytes.toBytes(item_purch_level));

      String item_gender = random.nextInt(2) < 1 ? "male" : "female";
      itemFeature.put("item_gender", item_gender);
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("item_gender"), Bytes.toBytes(item_gender));

      String item_buyer_age = String.valueOf(random.nextInt(8));
      itemFeature.put("item_buyer_age", item_buyer_age);
      put.addColumn(
          Bytes.toBytes("cf"), Bytes.toBytes("item_buyer_age"), Bytes.toBytes(item_buyer_age));

      String item_style_id = String.valueOf(random.nextInt(20));
      itemFeature.put("item_style_id", item_style_id);
      put.addColumn(
          Bytes.toBytes("cf"), Bytes.toBytes("item_style_id"), Bytes.toBytes(item_style_id));

      String item_material_id = String.valueOf(random.nextInt(20));
      itemFeature.put("item_material_id", item_material_id);
      put.addColumn(
          Bytes.toBytes("cf"), Bytes.toBytes("item_material_id"), Bytes.toBytes(item_material_id));

      String item_pay_class = String.valueOf(random.nextInt(5));
      itemFeature.put("item_pay_class", item_pay_class);
      put.addColumn(
          Bytes.toBytes("cf"), Bytes.toBytes("item_pay_class"), Bytes.toBytes(item_pay_class));

      String item_brand_id = String.valueOf(random.nextInt(100));
      itemFeature.put("item_brand_id", item_brand_id);
      put.addColumn(
          Bytes.toBytes("cf"), Bytes.toBytes("item_brand_id"), Bytes.toBytes(item_brand_id));

      String item__i_shop_id_ctr = String.valueOf(random.nextInt(100) / 10000.0);
      itemFeature.put("item__i_shop_id_ctr", item__i_shop_id_ctr);
      put.addColumn(
          Bytes.toBytes("cf"),
          Bytes.toBytes("item__i_shop_id_ctr"),
          Bytes.toBytes(item__i_shop_id_ctr));

      String item__i_brand_id_ctr = String.valueOf(random.nextInt(200) / 10000.0);
      itemFeature.put("item__i_brand_id_ctr", item__i_brand_id_ctr);
      put.addColumn(
          Bytes.toBytes("cf"),
          Bytes.toBytes("item__i_brand_id_ctr"),
          Bytes.toBytes(item__i_brand_id_ctr));

      String item__i_category_ctr = String.valueOf(random.nextInt(70) / 10000.0);
      itemFeature.put("item__i_category_ctr", item__i_category_ctr);
      put.addColumn(
          Bytes.toBytes("cf"),
          Bytes.toBytes("item__i_category_ctr"),
          Bytes.toBytes(item__i_category_ctr));

      table.put(put);

      System.out.println(itemFeature);
    }
    connection.close();
  }
}
