package com.globant.training.hadoop.loganalyzer.helper;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;

/**
 * UDF Function for the pig script, used to scalate de size bytes
 * Created by armandodelgado.
 */
public class ScaledSize extends EvalFunc<String> {

    /**
     * @param input Tuple with the value and unit to scalate
     * @return String with the size and unit scalated
     * @throws IOException
     */
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() < 2 || input.get(0) == null || input.get(1) == null)
            return null;
        try {
            double size = (Double) input.get(0);
            String scale = (String) input.get(1);

            String scaledSize;
            if (scale == null || scale.equals("")) {
                scaledSize = Double.toString(size) + "b";
            } else if (scale.equals("K")) {
                scaledSize = Double.toString(size / 1024) + "Kb";
            } else if (scale.equals("M")) {
                scaledSize = Double.toString((size / 1024) / 1024) + "Mb";
            } else if (scale.equals("G")) {
                scaledSize = Double.toString(((size / 1024) / 1024) / 1024) + "Gb";
            } else {
                scaledSize = Double.toString(size) + "b";
            }

            return scaledSize.toUpperCase();
        } catch (Exception e) {
            throw new IOException("Caught exception processing input row ", e);
        }
    }
}
