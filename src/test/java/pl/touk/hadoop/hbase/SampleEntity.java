package pl.touk.hadoop.hbase;

import org.springframework.beans.factory.annotation.Value;
import pl.touk.hadoop.hbase.annotation.Column;
import pl.touk.hadoop.hbase.annotation.Id;
import pl.touk.hadoop.hbase.annotation.Table;

@Table(AnnotationsIntegrationTest.TEST_TABLE)
public class SampleEntity {


    private String id;
    private String name;

    private int counter;

    public String getId() {
        return id;
    }

    @Id
    @Value("split('\\^')[0]")
    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    @Column(AnnotationsIntegrationTest.FAMILY_NAME + ":" + AnnotationsIntegrationTest.COLUMN_NAME)
    public void setName(String name) {
        this.name = name;
    }

    public int getCounter() {
        return counter;
    }

    @Column(AnnotationsIntegrationTest.FAMILY_NAME + ":b")
    public void setCounter(int counter) {
        this.counter = counter;
    }
}
