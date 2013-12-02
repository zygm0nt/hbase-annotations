package pl.touk.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;

public class SampleHBaseClient extends BaseHadoopInteraction<SampleEntity>{

    public SampleHBaseClient(Configuration configuration) {
        super(SampleEntity.class, configuration);
    }
}
