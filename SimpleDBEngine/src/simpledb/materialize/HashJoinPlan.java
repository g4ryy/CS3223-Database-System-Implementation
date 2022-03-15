package simpledb.materialize;

import simpledb.tx.Transaction;
import simpledb.plan.Plan;
import simpledb.query.*;
import simpledb.record.*;

import java.util.*;


/**
 * The Plan class for the <i>hashjoin</i> operator.
 */
public class HashJoinPlan implements Plan {
    private Transaction tx;
    private Plan smallPlan, largePlan;
    private String smallField, largeField;
    private Schema sch = new Schema();
    private int numParitions;

    /**
     * Creates a hashjoin plan for the two specified queries.
     *
     * @param tx the calling transaction
     * @param p1 the LHS query plan
     * @param p2 the RHS query plan
     * @param fldname1 the LHS join field
     * @param fldname2 the RHS join field
     */
    public HashJoinPlan(Transaction tx, Plan p1, Plan p2, String fldname1, String fldname2) {
        this.tx = tx;
        if (p1.recordsOutput() >= p2.recordsOutput()) {
            this.smallPlan = p2;
            this.largePlan = p1;
            this.smallField = fldname2;
            this.largeField = fldname1;
        } else {
            this.smallPlan = p1;
            this.largePlan = p2;
            this.smallField = fldname1;
            this.largeField = fldname2;
        }
        this.numParitions = tx.availableBuffs() - 1;
        assert numParitions >= 3 : "Less than 3 buffers available!";

        sch.addAll(p1.schema());
        sch.addAll(p2.schema());
    }

    /**
     * The method first generates partitions for the two tables.
     * It then returns a hashjoin scan
     * of the two partitioned table scans.
     * @see simpledb.plan.Plan#open()
     */
    public Scan open() {
        Map<Integer, TempTable> smallPartitions = getPartitions(smallPlan, smallField);
        Map<Integer, TempTable> largePartitions = getPartitions(largePlan, largeField);
        return new HashJoinScan(smallPartitions, largePartitions, smallField, largeField, smallPlan.schema());
    }

    private Map<Integer, TempTable> getPartitions(Plan p, String joinField) {
        Scan src = p.open();
        Map<Integer, TempTable> output = new HashMap<>();
        Map<Integer, List<Map<String, Constant>>> temps = new HashMap<>();

        src.beforeFirst();

        boolean hasRecord = src.next();

        if (!hasRecord) {
            return output;
        }

        while (hasRecord) {
            int partition = -1;

            Map<String, Constant> map = new HashMap<>();

            for (String fldname : p.schema().fields()) {
                Constant value = src.getVal(fldname);
                map.put(fldname, value);

                if (fldname.equals(joinField)) {
                    partition = value.hashCode() % numParitions;
                }
            }

            assert partition >= 0;

            if (!temps.containsKey(partition)) {
                temps.put(partition, new ArrayList<>());
            }

            temps.get(partition).add(map);

            hasRecord = src.next();
        }

        for (Integer partitionNum : temps.keySet()) {
            TempTable currenttemp = new TempTable(tx, p.schema());
            UpdateScan currentscan = currenttemp.open();

            for (Map<String, Constant> record : temps.get(partitionNum)) {
                currentscan.insert();
                for (String fldname : p.schema().fields())
                    currentscan.setVal(fldname, record.get(fldname));
            }
            currentscan.close();
            output.put(partitionNum, currenttemp);
        }

        return output;
    }

    /**
     * Estimates the number of block accesses to compute the join.
     * The formula is:
     * <pre> B(hashjoin(p1,p2)) = 3 * (B(p1) + B(p2))</pre>
     * @see simpledb.plan.Plan#blocksAccessed()
     */
    public int blocksAccessed() {
        return 3 * (smallPlan.blocksAccessed() + largePlan.blocksAccessed());
    }

    /**
     * Estimates the number of output records in the join.
     * The formula is:
     * <pre> R(hashjoin(p1,p2)) = R(p1)*R(p2)/max{V(p1,F1),V(p2,F2)} </pre>
     * @see simpledb.plan.Plan#recordsOutput()
     */
    public int recordsOutput() {
        int maxvals = Math.max(smallPlan.distinctValues(smallField),
            largePlan.distinctValues(largeField));
        return (smallPlan.recordsOutput() * largePlan.recordsOutput()) / maxvals;
    }

    /**
     * Estimate the distinct number of field values in the join.
     * Since the join does not increase or decrease field values,
     * the estimate is the same as in the appropriate underlying query.
     * @see simpledb.plan.Plan#distinctValues(java.lang.String)
     */
    public int distinctValues(String fldname) {
        if (smallPlan.schema().hasField(fldname))
            return smallPlan.distinctValues(fldname);
        else
            return largePlan.distinctValues(fldname);
    }

    /**
     * Return the schema of the join,
     * which is the union of the schemas of the underlying queries.
     * @see simpledb.plan.Plan#schema()
     */
    public Schema schema() {
        return sch;
    }
}

