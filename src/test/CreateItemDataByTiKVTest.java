import com.alibaba.streamcompute.data.tikv.CreateItemDataByTiKV;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class CreateItemDataByTiKVTest {

    private CreateItemDataByTiKV test;

    @Test
    public void main() throws IOException {
    }

    @Test
    public void generate() throws Exception {
//        test = new CreateItemDataByTiKV();
        test.generate(200);
    }
}