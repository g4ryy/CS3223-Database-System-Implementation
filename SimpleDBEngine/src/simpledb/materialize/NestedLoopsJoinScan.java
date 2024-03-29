package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Operator;
import simpledb.query.Scan;

/**
 * The Scan class for the <i>nested loops join</i> operator.
 *
 * @author Edward Sciore
 */
public class NestedLoopsJoinScan implements Scan {
    private Scan s1, s2;
    private String fldname1, fldname2;
    private boolean s1IsEmpty;
    private boolean s2IsEmpty;
    private Operator opr;

    /**
     * Create a nested loops join scan for the two underlying sorted scans.
     *
     * @param s1       the LHS scan
     * @param s2       the RHS scan
     * @param fldname1 the LHS join field
     * @param fldname2 the RHS join field
     * @param opr      the operator used to compare the two fields
     */
    public NestedLoopsJoinScan(Scan s1, Scan s2, String fldname1, String fldname2, Operator opr) {
        this.s1 = s1;
        this.s2 = s2;
        this.fldname1 = fldname1;
        this.fldname2 = fldname2;
        this.opr = opr;
        s1IsEmpty = !s1.next();
        s2IsEmpty = !s2.next();
        beforeFirst();
    }

    /**
     * Close the scan by closing the two underlying scans.
     *
     * @see simpledb.query.Scan#close()
     */
    public void close() {
        s1.close();
        s2.close();
    }

    /**
     * Position the lhs scan at the first record, and the rhs scan before
     * the first record.
     *
     * @see simpledb.query.Scan#beforeFirst()
     */
    public void beforeFirst() {
        s1.beforeFirst();
        if (!s1IsEmpty) {
            s1.next();
        }
        s2.beforeFirst();
    }

    /**
     * @see simpledb.query.Scan#next()
     */
    public boolean next() {
        if (s1IsEmpty || s2IsEmpty) {
            return false;
        }
        while (s2.next()) {
            Constant v1 = s1.getVal(fldname1);
            Constant v2 = s2.getVal(fldname2);
            if (!opr.evaluate(v1, v2)) {
                continue;
            } else {
                return true;
            }
        }
        s2.beforeFirst();
        if (!s1.next()) {
            return false;
        }
        return next();
    }

    /**
     * Return the integer value of the specified field.
     * The value is obtained from whichever scan
     * contains the field.
     *
     * @see simpledb.query.Scan#getInt(java.lang.String)
     */
    public int getInt(String fldname) {
        if (s1.hasField(fldname))
            return s1.getInt(fldname);
        else
            return s2.getInt(fldname);
    }

    /**
     * Return the string value of the specified field.
     * The value is obtained from whichever scan
     * contains the field.
     *
     * @see simpledb.query.Scan#getString(java.lang.String)
     */
    public String getString(String fldname) {
        if (s1.hasField(fldname))
            return s1.getString(fldname);
        else
            return s2.getString(fldname);
    }

    /**
     * Return the value of the specified field.
     * The value is obtained from whichever scan
     * contains the field.
     *
     * @see simpledb.query.Scan#getVal(java.lang.String)
     */
    public Constant getVal(String fldname) {
        if (s1.hasField(fldname))
            return s1.getVal(fldname);
        else
            return s2.getVal(fldname);
    }

    /**
     * Return true if the specified field is in
     * either of the underlying scans.
     *
     * @see simpledb.query.Scan#hasField(java.lang.String)
     */
    public boolean hasField(String fldname) {
        return s1.hasField(fldname) || s2.hasField(fldname);
    }
}

