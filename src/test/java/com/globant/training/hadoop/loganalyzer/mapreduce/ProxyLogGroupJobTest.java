package com.globant.training.hadoop.loganalyzer.mapreduce;

import com.globant.training.hadoop.loganalyzer.model.ProxyGroupedData;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by armandodelgado.
 */
public class ProxyLogGroupJobTest {
    MapDriver<LongWritable, Text, Text, ProxyGroupedData> mapDriver;
    ReduceDriver<Text, ProxyGroupedData, Text, ProxyGroupedData> reduceDriver;

    @Before
    public void setUp() {
        ProxyLogGroupMapper mapper = new ProxyLogGroupMapper();
        ProxyLogGroupReducer reducer = new ProxyLogGroupReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void testMapper() throws IOException {
        LongWritable inKey = new LongWritable(0);
        Text inValue = new Text("2015:01:13-14:23:58 AR-BADC-FAST-01 httpproxy[27983]: id=\"0001\" severity=\"info\" sys=\"SecureWeb\" sub=\"http\" name=\"http access\" action=\"pass\" method=\"GET\" srcip=\"10.20.11.138\" dstip=\"74.217.76.7\" user=\"\" ad_domain=\"\" statuscode=\"200\" cached=\"0\" profile=\"REF_DefaultHTTPProfile (Default Web Filter Profile)\" filteraction=\"REF_DefaultHTTPCFFAction (Default content filter action)\" size=\"8363\" request=\"0x358a2600\" url=\"http://www.cloudera.com/content/dam/cloudera/logos/customers/homepage_customer_logo_banner/persado-logo.png\" exceptions=\"\" error=\"\" authtime=\"0\" dnstime=\"67\" cattime=\"310111\" avscantime=\"0\" fullreqtime=\"762134\" device=\"0\" auth=\"0\" category=\"105,175\" reputation=\"trusted\" categoryname=\"Business,Software/Hardware\" content-type=\"image/png\"");
        mapDriver.withInput(inKey, inValue);

        Text outKey = new Text("www.cloudera.com");
        ProxyGroupedData outValue = new ProxyGroupedData(8363, 762134, 1);
        mapDriver.withOutput(outKey, outValue);
        mapDriver.runTest(false);
    }

    @Test
    public void testReducer() throws IOException {
        Text inKey = new Text("www.cloudera.com");
        List<ProxyGroupedData> inListValues = new ArrayList<ProxyGroupedData>();
        ProxyGroupedData inValue1 = new ProxyGroupedData(1024, 360, 1);
        ProxyGroupedData inValue2 = new ProxyGroupedData(1024, 360, 3);
        inListValues.add(inValue1);
        inListValues.add(inValue2);
        reduceDriver.withInput(inKey, inListValues);

        Text outKey = new Text("www.cloudera.com");
        ProxyGroupedData outValue = new ProxyGroupedData(1024, 360, 4);

        reduceDriver.withOutput(outKey, outValue);
        reduceDriver.runTest(false);
    }
}
