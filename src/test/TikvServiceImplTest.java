import com.alibaba.streamcompute.impl.TikvServiceImpl;
import com.alibaba.streamcompute.service.TiKVStorageService;
import com.alibaba.streamcompute.tools.Constants;
import org.junit.Test;
import org.tikv.kvproto.Kvrpcpb;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;


public class TikvServiceImplTest {

    private TiKVStorageService storageService = new TikvServiceImpl(Constants.PD_ADDRESS);

    @Test
    public void scanData() throws Exception {
        List<Kvrpcpb.KvPair> result = (List<Kvrpcpb.KvPair>) storageService.scanData("user",new ArrayList<>());
        System.out.println("---------------------------------------------user result.size:" + result.size() + "-------------------------------------");
        for(Kvrpcpb.KvPair item: result){
            System.out.println(item.getKey().toStringUtf8()+'\t'+item.getValue().toStringUtf8());
        }


//        ArrayList<Map<String, String>> userFilterInfos = new ArrayList<>();
//        userFilterInfos.add(getFilterInfo("user_id", "3", "cf"));
//        result = (List<Kvrpcpb.KvPair>) storageService.scanData("user",userFilterInfos);
//        System.out.println("---------------------------------------------result.size:" + result.size() + "-------------------------------------");
//        for(Kvrpcpb.KvPair item: result)
//            System.out.println(item.getKey().toStringUtf8()+'\t'+item.getValue().toStringUtf8());

        result = (List<Kvrpcpb.KvPair>) storageService.scanData("item",new ArrayList<>());
        System.out.println("---------------------------------------------item result.size:" + result.size() + "-------------------------------------");
        for(Kvrpcpb.KvPair item: result) {
            System.out.println(item.getKey().toStringUtf8()+'\t'+item.getValue().toStringUtf8());
        }

        result = (List<Kvrpcpb.KvPair>) storageService.scanData("click",new ArrayList<>());
        System.out.println("---------------------------------------------click result.size:" + result.size() + "-------------------------------------");
        for(Kvrpcpb.KvPair item: result) {
            System.out.println(item.getKey().toStringUtf8() + '\t' + item.getValue().toStringUtf8());
        }
//
//        userFilterInfos.remove(0);
//        userFilterInfos.add(getFilterInfo("item_id", "3", "cf"));
//        result = (List<Kvrpcpb.KvPair>) storageService.scanData("item",userFilterInfos);
//        System.out.println("---------------------------------------------result.size:" + result.size() + "-------------------------------------");
//        for(Kvrpcpb.KvPair item: result)
//            System.out.println(item.getKey().toStringUtf8()+'\t'+item.getValue().toStringUtf8());
//
//        userFilterInfos.remove(0);
//        userFilterInfos.add(getFilterInfo("flag", "1", "cf"));
//        result = (List<Kvrpcpb.KvPair>) storageService.scanData("click", userFilterInfos);
//        System.out.println("---------------------------------------------result.size:" + result.size() + "-------------------------------------");
//        for(Kvrpcpb.KvPair item: result)
//            System.out.println(item.getKey().toStringUtf8()+'\t'+item.getValue().toStringUtf8());

//        userFilterInfos.remove(0);
//        userFilterInfos.add(getFilterInfo("user_id", "1", "cf"));
//        result = (List<Kvrpcpb.KvPair>) storageService.scanData("click", userFilterInfos);
//        System.out.println("---------------------------------------------result.size:" + result.size() + "-------------------------------------");
//        for(Kvrpcpb.KvPair item: result)
//            System.out.println(item.getKey().toStringUtf8()+'\t'+item.getValue().toStringUtf8());

    }

    @Test
    public void writeDataWithJSON() {
    }

    @Test
    public void writeData() {
    }

    @Test
    public void createUniqueIndex() {
    }

    @Test
    public void createNoUniqueIndex() {
    }

    @Test
    public void updateI2i() {
    }

    @Test
    public void getDataByUniqueIndexKey() {
        String value = storageService.getDataByUniqueIndexKey("user","user_id","1");
        System.out.println("user_id=1" + '\t' + value);
    }

    @Test
    public void getDataByNoUniqueIndexKey() {
    }

    @Test
    public void getI2i() {
    }

    @Test
    public void getItemIds() throws Exception {
        Set<String> itemIds = storageService.getItemIds();
        System.out.println("---------------------------------------------itemIds.size:" + itemIds.size() + "-------------------------------------");
        for(String item:itemIds)
            System.out.println(item);
    }

    @Test
    public void getUserClickRecord() {
    }

    @Test
    public void generateSample() {
    }

    @Test
    public void createTiKVClient(){
        storageService.createTiKVClient();
    }

    @Test
    public void deleteAllData() throws Exception {
        storageService.deleteDataByTableName("user");
        storageService.deleteDataByTableName("item");
        storageService.deleteDataByTableName("click");
        storageService.deleteIndexByTableName("user");
        storageService.deleteIndexByTableName("item");
        storageService.deleteIndexByTableName("click");
    }

    @Test
    public void loadToTiKVFromHBaseTest() throws Exception {
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

        List<String> click_feature = new ArrayList<>();
        click_feature.add("user_id");
        click_feature.add("item_id");
        click_feature.add("flag");
        click_feature.add("date");

        storageService.loadToTiKVFromHBase("item",item_features);
    }
}