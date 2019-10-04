package com.alibaba.streamcompute.data;

import com.alibaba.streamcompute.tools.HBaseUtil;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

// 创建用户数据
public class CreateUserData {
  public static void main(String[] args) throws IOException {

    List<String> features = new ArrayList<>();
    features.add("user_id");
    features.add("pred_gender");
    features.add("pred_age_level");
    features.add("pred_career_type");
    features.add("pred_education_degree");
    features.add("pred_baby_age");
    features.add("pred_has_pet");
    features.add("pred_has_car");
    features.add("pred_life_stage");
    features.add("pred_has_house");
    features.add("os");
    //        features.add("purchase_total");
    //        features.add("se_cate_level1_prefer");
    //        features.add("se_cate_leaf_prefer");
    //        features.add("clk_cate_level1_prefer");
    //        features.add("clk_cate_leaf_prefer");
    //        features.add("brand_prefer");
    //        features.add("shop_prefer");
    //        features.add("resolution");
    //        features.add("l1cat_long_score");
    //        features.add("leafcat_long_score");
    //        features.add("category_ctr_1");
    //        features.add("category_ctr_3");
    //        features.add("category_ctr_7");
    //        features.add("category_level1_ctr_1");
    //        features.add("category_level1_ctr_3");
    //        features.add("category_level1_ctr_7");
    //        features.add("history_items");

    generate(50);
  }

  // 创建一个user表，该表是由一个"cf"列族，以及由11个特征作为列来形成的，generate方法会向该表插入50条记录
  public static void generate(int num) throws IOException {

    Connection connection = HBaseUtil.getHbaseConnection();
    HTable table = (HTable) connection.getTable(TableName.valueOf("user"));

    Random random = new Random();
    List<String> careers =
        Arrays.asList(
            "R&D personnel",
            "civilian staff",
            "Counter person",
            "professor",
            "Designer",
            "Financial officer",
            "judge",
            "lawyer",
            "Clerk",
            "Guard",
            "editor",
            "Doctors",
            "nurse",
            "engineer",
            "Laboratory staff");
    List<String> educations =
        Arrays.asList(
            "Postgraduate",
            "Undergraduate",
            "University specialties",
            "specialized middle school",
            "Technical",
            "High school",
            "junior high school",
            "primary school",
            "illiteracy");

    for (int i = 0; i < num; i++) {

      Put put = new Put(Bytes.toBytes(String.valueOf(i)));

      Map<String, String> userFeature = new HashMap<>();

      userFeature.put("user_id", String.valueOf(i));
      put.addColumn(
          Bytes.toBytes("cf"), Bytes.toBytes("user_id"), Bytes.toBytes(String.valueOf(i)));

      String gender = random.nextInt(1) < 1 ? "male" : "famale";
      userFeature.put("pred_gender", gender);
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("pred_gender"), Bytes.toBytes(gender));

      String age = String.valueOf(random.nextInt(8));
      userFeature.put("pred_age_level", age);
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("pred_age_level"), Bytes.toBytes(age));

      String career = careers.get(random.nextInt(careers.size()));
      userFeature.put("pred_career_type", career);
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("pred_career_type"), Bytes.toBytes(career));

      String education = educations.get(random.nextInt(educations.size()));
      userFeature.put("pred_education_degree", education);
      put.addColumn(
          Bytes.toBytes("cf"), Bytes.toBytes("pred_education_degree"), Bytes.toBytes(education));

      int bage = random.nextInt(Integer.valueOf(userFeature.get("pred_age_level")) + 1);
      String babyAge = String.valueOf(bage - 3 < 0 ? "-1" : bage);
      userFeature.put("pred_baby_age", babyAge);
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("pred_baby_age"), Bytes.toBytes(babyAge));

      String pet = random.nextInt(10) < 2 ? "yes" : "no";
      userFeature.put("pred_has_pet", pet);
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("pred_has_pet"), Bytes.toBytes(pet));

      String car = random.nextInt(10) < 7 ? "yes" : "no";
      userFeature.put("pred_has_car", car);
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("pred_has_car"), Bytes.toBytes(car));

      String life = random.nextInt(2) < 1 ? "married" : "unmarried";
      userFeature.put("pred_life_stage", life);
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("pred_life_stage"), Bytes.toBytes(life));

      String house = random.nextInt(8) < 3 ? "yes" : "no";
      userFeature.put("pred_has_house", house);
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("pred_has_house"), Bytes.toBytes(house));

      String os = random.nextInt(10) < 3 ? "apple" : "android";
      userFeature.put("os", os);
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("os"), Bytes.toBytes(os));

      table.put(put);
      System.out.println(userFeature);
    }
    connection.close();
  }
}
