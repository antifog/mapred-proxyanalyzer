package com.globant.training.hadoop.loganalyzer.mapreduce;

import com.globant.training.hadoop.loganalyzer.model.VisitsSiteKey;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Grouping comparator to be used as in the Secondary Sort mechanism
 * to be sure the reduce() method gets the data grouped by visit counter
 * Created by armandodelgado.
 */
public class VisitsCounterGroupingComparator extends WritableComparator {

    public VisitsCounterGroupingComparator() {
        super(VisitsSiteKey.class, true);
    }

    /* (non-Javadoc)
     * @see java.util.Comparator#compare(T o1, T o2)
     */
    @Override
    public int compare(WritableComparable skr1, WritableComparable skr2) {
        VisitsSiteKey visitsSiteKey1 = (VisitsSiteKey) skr1;
        VisitsSiteKey visitsSiteKey2 = (VisitsSiteKey) skr2;

        return visitsSiteKey1.getVisitsCounter().compareTo(visitsSiteKey2.getVisitsCounter());
    }
}
