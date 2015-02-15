package com.globant.training.hadoop.loganalyzer.mapreduce;

import com.globant.training.hadoop.loganalyzer.model.ProxyGroupedData;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer used as a combiner and reducer, used to aggregate the average values and the visitCounter
 * the average calculations are based in the current number of visits by site domain
 * Created by armandodelgado.
 */
public class ProxyLogGroupReducer extends Reducer<Text, ProxyGroupedData, Text, ProxyGroupedData> {

    @Override
    protected void reduce(Text key, Iterable<ProxyGroupedData> values, Context context) throws IOException, InterruptedException {

        double sizeAgg = 0;
        double rtAgg = 0;
        long visitsCounter = 0;

        // Some calculation is needed in order to avoid problems calculating the average values
        for (ProxyGroupedData logData : values) {
            sizeAgg += (logData.getBytesAvg().get() * logData.getVisitsCounter().get());
            rtAgg += (logData.getResponseTimeAvg().get() * logData.getVisitsCounter().get());
            visitsCounter += logData.getVisitsCounter().get();
        }

        ProxyGroupedData output = new ProxyGroupedData(
                sizeAgg / visitsCounter,
                rtAgg / visitsCounter,
                visitsCounter
        );

        context.write(key, output);
    }
}
