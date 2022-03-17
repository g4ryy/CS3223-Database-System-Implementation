package simpledb.materialize;

import java.util.Comparator;
import java.util.List;

import simpledb.query.Constant;
import simpledb.query.OrderField;
import simpledb.query.Scan;

/**
 * A comparator for scans.
 *
 * @author Edward Sciore
 */
public class RecordComparator implements Comparator<Scan> {
    private List<OrderField> fields;

    /**
     * Create a comparator using the specified fields,
     * using the ordering implied by its iterator.
     *
     * @param fields a list of field names
     */
    public RecordComparator(List<OrderField> fields) {
        this.fields = fields;
    }

    /**
     * Compare the current records of the two specified scans.
     * The sort fields are considered in turn.
     * When a field is encountered for which the records have
     * different values, those values are used as the result
     * of the comparison.
     * If the two records have the same values for all
     * sort fields, then the method returns 0.
     *
     * @param s1 the first scan
     * @param s2 the second scan
     * @return the result of comparing each scan's current record according to the field list
     */
    public int compare(Scan s1, Scan s2) {
        for (OrderField orderField : fields) {
            String fieldName = orderField.getField();
            String orderType = orderField.getType();
            Constant val1 = s1.getVal(fieldName);
            Constant val2 = s2.getVal(fieldName);
            int result = val1.compareTo(val2);
            if (orderType.equals("desc")) {
                result = -result;
            }
            if (result != 0)
                return result;
        }
        return 0;
    }
}
