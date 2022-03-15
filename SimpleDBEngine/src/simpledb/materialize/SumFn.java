package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;

/**
 * The <i>sum</i> aggregation function.
 */
public class SumFn implements AggregationFn {
    private String fldname;
    private int sum;

    /**
     * Create a sum aggregation function for the specified field.
     * @param fldname the name of the aggregated field
     */
    public SumFn(String fldname) {
        this.fldname = fldname;
    }

    /**
     * Start a new sum to be the field value in the current record.
     * Since SimpleDB does not support null values,
     * every record will be counted,
     * regardless of the field.
     * @see simpledb.materialize.AggregationFn#processFirst(Scan)
     */
    public void processFirst(Scan s) {
        sum = s.getInt(fldname);
    }

    /**
     * Since SimpleDB does not support null values,
     * this method always adds the field value in the current record to sum,
     * regardless of the field.
     * @see simpledb.materialize.AggregationFn#processNext(Scan)
     */
    public void processNext(Scan s) {
        sum += s.getInt(fldname);
    }

    /**
     * Return the field's name, prepended by "sumof".
     * @see simpledb.materialize.AggregationFn#fieldName()
     */
    public String fieldName() {
        return "sumof" + fldname;
    }

    /**
     * Return the current sum.
     * @see simpledb.materialize.AggregationFn#value()
     */
    public Constant value() {
        return new Constant(sum);
    }

    public String getField() {
        return fldname;
    }
}