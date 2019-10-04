package com.alibaba.streamcompute.data.tikv;

import com.alibaba.streamcompute.impl.TikvServiceImpl;
import com.alibaba.streamcompute.service.TiKVStorageService;
import com.alibaba.streamcompute.tools.Constants;
import java.io.IOException;
import java.util.*;

public class CreateUserDataByTiKV {
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

    generate(50);
  }

  // 创建一个user表，该表是由一个"cf"列族，以及由11个特征作为列来形成的，generate方法会向该表插入50条记录
  public static void generate(int num) throws IOException {
    //    String PD_ADDRESS = "127.0.0.1:2379";
    TiKVStorageService storageService = new TikvServiceImpl(Constants.PD_ADDRESS); // 创建Tikv服务

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
    // 循环插入user表的KV数据
    String tableName = "user";
    int rowid;
    boolean isIndexCreated;
    for (int i = 0; i < num; i++) {
      Map<String, String> userFeature = new HashMap<>();

      userFeature.put("user_id", String.valueOf(i));

      String gender = random.nextInt(1) < 1 ? "male" : "famale";
      userFeature.put("pred_gender", gender);

      String age = String.valueOf(random.nextInt(8));
      userFeature.put("pred_age_level", age);

      String career = careers.get(random.nextInt(careers.size()));
      userFeature.put("pred_career_type", career);

      String education = educations.get(random.nextInt(educations.size()));
      userFeature.put("pred_education_degree", education);

      int bage = random.nextInt(Integer.valueOf(userFeature.get("pred_age_level")) + 1);
      String babyAge = String.valueOf(bage - 3 < 0 ? "-1" : bage);
      userFeature.put("pred_baby_age", babyAge);

      String pet = random.nextInt(10) < 2 ? "yes" : "no";
      userFeature.put("pred_has_pet", pet);

      String car = random.nextInt(10) < 7 ? "yes" : "no";
      userFeature.put("pred_has_car", car);

      String life = random.nextInt(2) < 1 ? "married" : "unmarried";
      userFeature.put("pred_life_stage", life);

      String house = random.nextInt(8) < 3 ? "yes" : "no";
      userFeature.put("pred_has_house", house);

      String os = random.nextInt(10) < 3 ? "apple" : "android";
      userFeature.put("os", os);
      // 调用writeData插入表数据
      rowid = storageService.writeData(tableName, userFeature);
      // String tableName, String index_name, String index_value,int rowid
      String index_name = "user_id";
      String index_value = String.valueOf(i);
      // 调用createUniqueIndex创建索引
      isIndexCreated = storageService.createUniqueIndex(tableName, index_name, index_value, rowid);
      if (isIndexCreated) {
        System.out.println("user表成功创建索引！！");
      }
    }
  }
}
