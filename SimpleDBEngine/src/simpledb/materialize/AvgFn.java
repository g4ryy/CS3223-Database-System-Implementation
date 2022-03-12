package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;

/**
 * The <i>avg</i> aggregation function.
 */
public class AvgFn implements AggregationFn {
    private String fldname;
    private int count;
    private int sum;

    /**
     * Create an average aggregation function for the specified field.
     * @param fldname the name of the aggregated field
     */
    public AvgFn(String fldname) {
        this.fldname = fldname;
    }

    /**
     * Start a new count and start a new sum to be the field value in the current record.
     * Since SimpleDB does not support null values,
     * every record will be counted,
     * regardless of the field.
     * The current count is thus set to 1.
     * @see simpledb.materialize.AggregationFn#processFirst(Scan)
     */
    public void processFirst(Scan s) {
        sum = s.getInt(fldname);
        count = 1;
    }

    /**
     * Since SimpleDB does not support null values,
     * this method always adds the field value in the current record to sum,
     * and increments the count regardless of the field.
     * @see simpledb.materialize.AggregationFn#processNext(Scan)
     */
    public void processNext(Scan s) {
        sum += s.getInt(fldname);
        count ++;
    }

    /**
     * Return the field's name, prepended by "avgof".
     * @see simpledb.materialize.AggregationFn#fieldName()
     */
    public String fieldName() {
        return "avgof" + fldname;
    }

    /**
     * Return the current average.
     * @see simpledb.materialize.AggregationFn#value()
     */
    public Constant value() {
        return new Constant(sum/count);
    }
}