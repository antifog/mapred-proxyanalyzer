package com.globant.training.hadoop.loganalyzer.mapreduce;

import com.globant.training.hadoop.loganalyzer.helper.LogProxyReaderHelper;
import com.globant.training.hadoop.loganalyzer.model.ProxyGroupedData;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper to initialize the ProxyGroupedData, counting every line like an ocurrence for a site domain
 * Created by armandodelgado.
 */
public class ProxyLogGroupMapper extends Mapper<LongWritable, Text, Text, ProxyGroupedData> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        // Avoid the globant sites
        if (!line.contains("globant")) {

            // Create a ProxyGroupedData using the parse data
            // the average values are initialized in the only one value
            // and set the visitCounter to 1
            String[] extracted = LogProxyReaderHelper.getInstance().parseProxyLogInfo(line);
            if (extracted != null) {
                ProxyGroupedData proxyData = new ProxyGroupedData(
                        Double.parseDouble(extracted[0]),
                        Double.parseDouble(extracted[2]),
                        1
                );

                context.write(new Text(ProxyGroupedData.getSiteDomain(extracted[1])), proxyData);
            }
        }
    }
}
