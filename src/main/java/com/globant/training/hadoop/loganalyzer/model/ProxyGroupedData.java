package com.globant.training.hadoop.loganalyzer.model;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by armandodelgado.
 */
public class ProxyGroupedData implements Writable {

    // pattern used to parse the domain from the url
    private final static String siteDomainPattern = "(http|https)://(.*?)/.*";

    private DoubleWritable bytesAvg;
    private DoubleWritable responseTimeAvg;
    private LongWritable visitsCounter;

    /**
     * @param bytesAvg
     * @param responseTimeAvg
     * @param visitsCounter
     */
    public ProxyGroupedData(double bytesAvg, double responseTimeAvg, long visitsCounter) {
        this.bytesAvg = new DoubleWritable(bytesAvg);
        this.responseTimeAvg = new DoubleWritable(responseTimeAvg);
        this.visitsCounter = new LongWritable(visitsCounter);
    }

    /**
     * Default constructor used by mapreduce
     */
    public ProxyGroupedData() {
        this.bytesAvg = new DoubleWritable();
        this.responseTimeAvg = new DoubleWritable();
        this.visitsCounter = new LongWritable();
    }

    /**
     * @return the domain from the url
     */
    public static String getSiteDomain(String url) {
        Pattern siteExtract = Pattern.compile(siteDomainPattern);
        Matcher siteMatch = siteExtract.matcher(url);
        String siteDomain = "";

        if (siteMatch.matches())
            siteDomain = siteMatch.group(2);

        return siteDomain;
    }

    /**
     * @return the average size in bytes
     */
    public DoubleWritable getBytesAvg() {
        return bytesAvg;
    }

    /**
     * @param setBytesAvg
     */
    public void setBytesAvg(double setBytesAvg) {
        this.bytesAvg = new DoubleWritable(setBytesAvg);
    }

    /**
     * @param bytesAvg
     */
    public void setBytesAvg(DoubleWritable bytesAvg) {
        this.bytesAvg = bytesAvg;
    }

    /**
     * @return the average response time
     */
    public DoubleWritable getResponseTimeAvg() {
        return responseTimeAvg;
    }

    /**
     * @param setResponseTimeAvg
     */
    public void setResponseTimeAvg(double setResponseTimeAvg) {
        this.responseTimeAvg = new DoubleWritable(setResponseTimeAvg);
    }

    /**
     * @param responseTimeAvg
     */
    public void setResponseTimeAvg(DoubleWritable responseTimeAvg) {
        this.responseTimeAvg = responseTimeAvg;
    }

    /**
     * @return the counter for the visits to the site
     */
    public LongWritable getVisitsCounter() {
        return visitsCounter;
    }

    /**
     * @param visitsCounter
     */
    public void setVisitsCounter(long setVisitsCounter) {
        this.visitsCounter = new LongWritable(setVisitsCounter);
    }

    /**
     * @param visitsCounter
     */
    public void setVisitsCounter(LongWritable visitsCounter) {
        this.visitsCounter = visitsCounter;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.bytesAvg.readFields(in);
        this.responseTimeAvg.readFields(in);
        this.visitsCounter.readFields(in);
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    @Override
    public void write(DataOutput out) throws IOException {
        this.bytesAvg.write(out);
        this.responseTimeAvg.write(out);
        this.visitsCounter.write(out);
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return bytesAvg + "||" + responseTimeAvg + "||" + visitsCounter;
    }

    // Over written to be used in comparition for Unit Testing
    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ProxyGroupedData that = (ProxyGroupedData) o;
        return this.toString().equals(that.toString());
    }
}
