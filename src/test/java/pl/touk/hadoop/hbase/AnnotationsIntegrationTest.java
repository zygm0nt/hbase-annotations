package pl.touk.hadoop.hbase;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class AnnotationsIntegrationTest {

    public static final String TEST_TABLE = "test_table";
    public static final String FAMILY_NAME = "cf";
    public static final String COLUMN_NAME = "sampleColumn";
    public static final String SAMPLE_VALUE = "sample value";

    static HBaseClusterWrapper cluster;

    @BeforeClass
    public static void setup() throws Exception {
        cluster = new HBaseClusterWrapper();
    }

    @Test
    public void shouldTestSomething() throws Exception {
        prepareTestData();
        SampleHBaseClient client = new SampleHBaseClient(cluster.getConfig());
        SampleEntity entity = client.get(new Get(Bytes.toBytes("1")), TEST_TABLE);
        assertThat(entity).isNotNull();
        assertThat(entity.getName()).isEqualTo(SAMPLE_VALUE);
    }

    private void prepareTestData() throws IOException {
        HBaseAdmin admin = new HBaseAdmin(cluster.getConfig());
        if (admin.tableExists(TEST_TABLE)) {
            admin.disableTable(TEST_TABLE);
            admin.deleteTable(TEST_TABLE);
        }
        admin.createTable(getTableDescriptor());
        HTable table = new HTable(cluster.getConfig(), TEST_TABLE);
        Put testData = new Put(Bytes.toBytes("1"));
        testData.add(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes(COLUMN_NAME), Bytes.toBytes(SAMPLE_VALUE));
        table.put(testData);
    }

    private HTableDescriptor getTableDescriptor() {
        HTableDescriptor descriptor = new HTableDescriptor(TEST_TABLE);
        descriptor.addFamily(new HColumnDescriptor(FAMILY_NAME));
        return descriptor;
    }

    @AfterClass
    public static void teardown() throws Exception {
        cluster.shutdown();
    }

}
