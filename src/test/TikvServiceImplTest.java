import com.alibaba.streamcompute.impl.TikvServiceImpl;
import com.alibaba.streamcompute.service.TiKVStorageService;
import com.alibaba.streamcompute.tools.Constants;
import org.junit.Test;
import org.tikv.kvproto.Kvrpcpb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.streamcompute.tools.tikv.TikvUtil.getFilterInfo;
import static org.junit.Assert.*;

public class TikvServiceImplTest {

    private TiKVStorageService storageService = new TikvServiceImpl(Constants.PD_ADDRESS);

    @Test
    public void scanData() throws Exception {
        List<Kvrpcpb.KvPair> result = (List<Kvrpcpb.KvPair>) storageService.scanData("user",new ArrayList<>());
        System.out.println("---------------------------------------------result.size:" + result.size() + "-------------------------------------");
        for(Kvrpcpb.KvPair item: result)
            System.out.println(item.getKey().toStringUtf8()+'\t'+item.getValue().toStringUtf8());

        ArrayList<Map<String, String>> userFilterInfos = new ArrayList<>();
        userFilterInfos.add(getFilterInfo("user_id", "3", "cf"));
        result = (List<Kvrpcpb.KvPair>) storageService.scanData("user",userFilterInfos);
        System.out.println("---------------------------------------------result.size:" + result.size() + "-------------------------------------");
        for(Kvrpcpb.KvPair item: result)
            System.out.println(item.getKey().toStringUtf8()+'\t'+item.getValue().toStringUtf8());

        result = (List<Kvrpcpb.KvPair>) storageService.scanData("item",new ArrayList<>());
        System.out.println("---------------------------------------------result.size:" + result.size() + "-------------------------------------");
        for(Kvrpcpb.KvPair item: result)
            System.out.println(item.getKey().toStringUtf8()+'\t'+item.getValue().toStringUtf8());

        userFilterInfos.remove(0);
        userFilterInfos.add(getFilterInfo("item_id", "3", "cf"));
        result = (List<Kvrpcpb.KvPair>) storageService.scanData("item",userFilterInfos);
        System.out.println("---------------------------------------------result.size:" + result.size() + "-------------------------------------");
        for(Kvrpcpb.KvPair item: result)
            System.out.println(item.getKey().toStringUtf8()+'\t'+item.getValue().toStringUtf8());

        userFilterInfos.remove(0);
        userFilterInfos.add(getFilterInfo("flag", "1", "cf"));
        result = (List<Kvrpcpb.KvPair>) storageService.scanData("click", userFilterInfos);
        System.out.println("---------------------------------------------result.size:" + result.size() + "-------------------------------------");
        for(Kvrpcpb.KvPair item: result)
            System.out.println(item.getKey().toStringUtf8()+'\t'+item.getValue().toStringUtf8());

        userFilterInfos.remove(0);
        userFilterInfos.add(getFilterInfo("user_id", "1", "cf"));
        result = (List<Kvrpcpb.KvPair>) storageService.scanData("click", userFilterInfos);
        System.out.println("---------------------------------------------result.size:" + result.size() + "-------------------------------------");
        for(Kvrpcpb.KvPair item: result)
            System.out.println(item.getKey().toStringUtf8()+'\t'+item.getValue().toStringUtf8());

    }

    @Test
    public void writeDataWithJSON() {
    }

    @Test
    public void writeData() {
        System.out.println("test right");
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
}