package com.globant.training.hadoop.loganalyzer.mapreduce;

import com.globant.training.hadoop.loganalyzer.model.ProxyGroupedData;
import com.globant.training.hadoop.loganalyzer.model.VisitsSiteKey;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Mapper used to create VisitsSiteKey in order to be used in the "seconday sort" mechanism
 * Created by armandodelgado.
 */
public class ProxyLogSortMapper extends Mapper<LongWritable, Text, VisitsSiteKey, ProxyGroupedData> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // parse the values from the text line using tokenizer
        StringTokenizer stringTokenizer = new StringTokenizer(value.toString(), "||");
        if (stringTokenizer.countTokens() == 4) {

            String sitedomain = stringTokenizer.nextElement().toString();
            String bytesAvg = stringTokenizer.nextElement().toString();
            String responseTAvg = stringTokenizer.nextElement().toString();
            String visitsCounter = stringTokenizer.nextElement().toString();

            VisitsSiteKey visitsSiteKey = null;
            ProxyGroupedData proxyData = null;

            // Create a VisitsSiteKey for secondary sort
            visitsSiteKey = new VisitsSiteKey(Long.parseLong(visitsCounter), sitedomain);
            proxyData = new ProxyGroupedData(Double.parseDouble(bytesAvg),
                    Double.parseDouble(responseTAvg),
                    Long.parseLong(visitsCounter)
            );

            context.write(visitsSiteKey, proxyData);
        }
    }
}
