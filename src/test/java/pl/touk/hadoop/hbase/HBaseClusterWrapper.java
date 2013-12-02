package pl.touk.hadoop.hbase;

import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.LocalHBaseCluster;

public class HBaseClusterWrapper {

    private static LocalHBaseCluster miniCluster;
    private static TestingServer zkServer;

    private static Configuration config;

    public HBaseClusterWrapper() throws Exception {
        zkServer = new TestingServer();
        config = HBaseConfiguration.create();
        config.setInt("hbase.zookeeper.property.clientPort", zkServer.getPort());
        miniCluster = new LocalHBaseCluster(config, 1);
        miniCluster.startup();
    }

    public Configuration getConfig() {
        return config;
    }

    public void shutdown() throws Exception {
        miniCluster.shutdown();
        zkServer.close();
    }
}
