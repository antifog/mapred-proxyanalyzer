package com.globant.training.hadoop.loganalyzer.model;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Used as key to implement Secondary sort mechanism to avoid issues with to sites with the same visits#
 * Created by armandodelgado.
 */
public class VisitsSiteKey implements WritableComparable<VisitsSiteKey> {

    private LongWritable visitsCounter;
    private Text siteDomain;

    /**
     * Constructor
     *
     * @param visitsCounter
     * @param siteDomain
     */
    public VisitsSiteKey(long visitsCounter, String siteDomain) {
        this.visitsCounter = new LongWritable(visitsCounter);
        this.siteDomain = new Text(siteDomain);
    }

    /**
     * Default Constructor for mapreduce
     */
    public VisitsSiteKey() {
        this.visitsCounter = new LongWritable();
        this.siteDomain = new Text();
    }

    /**
     * @return visitsCounter
     */
    public LongWritable getVisitsCounter() {
        return visitsCounter;
    }

    /**
     * @param visitsCounter
     */
    public void setVisitsCounter(long visitsCounter) {
        this.visitsCounter = new LongWritable(visitsCounter);
    }

    /**
     * @param visitsCounter
     */
    public void setVisitsCounter(LongWritable visitsCounter) {
        this.visitsCounter = visitsCounter;
    }

    /**
     * @return site domain
     */
    public Text getSiteDomain() {
        return siteDomain;
    }

    /**
     * @param siteDomain
     */
    public void setSiteDomain(String siteDomain) {
        this.siteDomain = new Text(siteDomain);
    }

    /**
     * @param siteDomain
     */
    public void setSiteDomain(Text siteDomain) {
        this.siteDomain = siteDomain;
    }

    // Over written to be used when orderer the mapper output before
    // is passed to the reducer as in the Secondary sort mechanism
    /* (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    @Override
    public int compareTo(VisitsSiteKey siteRank) {
        int compareValue = this.visitsCounter.compareTo(siteRank.getVisitsCounter()) * -1;
        if (compareValue == 0) {
            compareValue = this.siteDomain.compareTo(siteRank.getSiteDomain());
        }
        return compareValue;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.visitsCounter.write(dataOutput);
        this.siteDomain.write(dataOutput);
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.visitsCounter.readFields(dataInput);
        this.siteDomain.readFields(dataInput);
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        VisitsSiteKey that = (VisitsSiteKey) o;

        if (visitsCounter != null ? !visitsCounter.equals(that.visitsCounter) : that.visitsCounter != null)
            return false;
        if (siteDomain != null ? !siteDomain.equals(that.siteDomain) : that.siteDomain != null) return false;

        return true;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode(java.lang.Object)
     */
    @Override
    public int hashCode() {
        int result = visitsCounter != null ? visitsCounter.hashCode() : 0;
        result = 127 * result + (siteDomain != null ? siteDomain.hashCode() : 0);
        return result;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return ((siteDomain != null && !siteDomain.toString().equals("")) ? siteDomain.toString() : "unknwsite");
    }
}
