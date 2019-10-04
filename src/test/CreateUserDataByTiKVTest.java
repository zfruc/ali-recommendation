import com.alibaba.streamcompute.data.tikv.CreateUserDataByTiKV;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class CreateUserDataByTiKVTest {

    private CreateUserDataByTiKV test;

    @Test
    public void generate() throws IOException {

        // generate 50 row into "user"
        test.generate(50);
    }
}