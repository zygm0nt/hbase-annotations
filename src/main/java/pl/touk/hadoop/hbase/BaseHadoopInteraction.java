package pl.touk.hadoop.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

public abstract class BaseHadoopInteraction<T> {

    public static final int TABLE_POOL = 10;

    protected HTablePool tablePool;
    private Configuration configuration;

    private AnnotationDrivenRowMapper<T> rowMapper;

    private Logger log = Logger.getLogger(BaseHadoopInteraction.class);

    protected BaseHadoopInteraction(Class<T> targetClass) {
        this(targetClass, HBaseConfiguration.create());
    }

    protected BaseHadoopInteraction(Class<T> targetClass, Configuration configuration) {
        this.rowMapper = new AnnotationDrivenRowMapper<T>(targetClass);
        this.configuration = configuration;
        this.tablePool = new HTablePool(configuration, TABLE_POOL);
    }

    protected List<T> scan(Scan scanCriteria, String tableName) {
        List<T> retVal = new ArrayList<T>();
        try {
            ResultScanner scanner = tablePool.getTable(tableName).getScanner(scanCriteria);
            Result result = scanner.next();
            while (result != null) {
                retVal.add(rowMapper.map(result));
                result = scanner.next();
            }
            scanner.close();
        } catch (Exception ex) {
            log.error("Error performing SCAN: ", ex);
        }
        return retVal;
    }

    /**
     * with paging
     */
    protected List<T> scan(Scan scanCriteria, int start, int limit, String tableName) {
        List<T> retVal = new ArrayList<T>();
        try {
            ResultScanner scanner = tablePool.getTable(tableName).getScanner(scanCriteria);
            Result result = scanner.next();
            int counter = 0;
            while (result != null && counter - start < limit) {
                if (counter >= start)
                    retVal.add(rowMapper.map(result));
                result = scanner.next();
                counter++;
            }
            scanner.close();
        } catch (Exception ex) {
            log.error("Error performing SCAN: ", ex);
        }

        return retVal;
    }

    protected T get(Get get, String tableName) {
        T object = null;
        try {
            Result result = tablePool.getTable(tableName).get(get);
            object = rowMapper.map(result);
        } catch (Exception e) {
            log.error("Error performing GET: ", e);
        }
        return object;
    }

    protected boolean delete(List<String> fileIdList, String tableName) {
        List<Delete> deletes = new ArrayList<Delete>();
        for (String id : fileIdList) deletes.add(new Delete(Bytes.toBytes(id)));
        try {
            tablePool.getTable(tableName).delete(deletes);
        } catch (IOException e) {
            log.error("Error performing DELETE: ", e);
            return false;
        }
        return true;
    }

    protected Configuration getConfiguration() {
        return configuration;
    }

    protected void reconnect() {
        try {
            tablePool.close();
            tablePool = new HTablePool();
        } catch (IOException e) {
            log.error("Error closing table pool", e);
        }
    }
}
