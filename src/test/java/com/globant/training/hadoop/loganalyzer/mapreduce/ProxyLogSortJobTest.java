package com.globant.training.hadoop.loganalyzer.mapreduce;

import com.globant.training.hadoop.loganalyzer.model.ProxyGroupedData;
import com.globant.training.hadoop.loganalyzer.model.VisitsSiteKey;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by armandodelgado.
 */
public class ProxyLogSortJobTest {
    MapDriver<LongWritable, Text, VisitsSiteKey, ProxyGroupedData> mapDriver;

    @Before
    public void setUp() {
        ProxyLogSortMapper mapper = new ProxyLogSortMapper();
        Reducer reducer = new Reducer();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testMapper() throws IOException {
        LongWritable inKey = new LongWritable(0);
        Text inValue = new Text("www.cloudera.com||2048||360||5");
        mapDriver.withInput(inKey, inValue);

        VisitsSiteKey outKey = new VisitsSiteKey(5, "www.cloudera.com");
        ProxyGroupedData outValue = new ProxyGroupedData(2048, 360, 5);
        mapDriver.withOutput(outKey, outValue);
        mapDriver.runTest();
    }
}
