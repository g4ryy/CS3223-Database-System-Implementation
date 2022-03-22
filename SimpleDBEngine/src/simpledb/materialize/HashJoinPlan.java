package simpledb.materialize;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import simpledb.plan.Plan;
import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;


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
     * @param tx       the calling transaction
     * @param p1       the LHS query plan
     * @param p2       the RHS query plan
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
     *
     * @see simpledb.plan.Plan#open()
     */
    public Scan open() {
        Map<Integer, TempTable> smallerPartitions = getPartitions(smallPlan.open(), smallPlan.schema(), smallField, 0);
        Map<Integer, TempTable> largerPartitions = getPartitions(largePlan.open(), largePlan.schema(), largeField, 0);

        List<Integer> toSplit;

        // split into more partitions if any partition too large
        while (true) {
            toSplit = new ArrayList<>();
            for (int i = 0; i < smallerPartitions.size(); i++) {
                int partitionsize = tx.size(smallerPartitions.get(i).tableName() + ".tbl");
                if (partitionsize > Math.max(tx.availableBuffs() - 2, 1)) {
                    toSplit.add(i);
                }
            }

            if (toSplit.size() == 0) {
                break;
            }

            for (int i : toSplit) {
                smallerPartitions.putAll(
                    getPartitions(
                        smallerPartitions.get(i).open(),
                        smallerPartitions.get(i).getLayout().schema(),
                        smallField, Collections.max(smallerPartitions.keySet())));

                largerPartitions.putAll(
                    getPartitions(
                        largerPartitions.get(i).open(),
                        largerPartitions.get(i).getLayout().schema(),
                        largeField, Collections.max(largerPartitions.keySet())));
            }

            for (int i : toSplit) {
                smallerPartitions.remove(i);
                largerPartitions.remove(i);
            }
        }

        return new HashJoinScan(smallerPartitions,smallField, largerPartitions,
            largeField);
    }

    private Map<Integer, TempTable> getPartitions(Scan s, Schema sch, String joinField, int start) {
        int partitions = tx.availableBuffs() - 1;

        Map<Integer, TempTable> ttList = new HashMap<>();
        Map<Integer, UpdateScan> scanList = new HashMap<>();

        for (int i = start; i < partitions + start; i++) {
            ttList.put(i, new TempTable(tx, sch));
            scanList.put(i, (UpdateScan) ttList.get(i).open());
        }
        while (s.next()) {
            UpdateScan dest = scanList.
                get((s.getVal(joinField).
                    hashCode() % partitions) + start);
            dest.insert();
            for (String fldname : sch.fields()) {
                dest.setVal(fldname, s.getVal(fldname));
            }
        }

        for (int i = start; i < partitions + start; i++) {
            scanList.get(i).close();
        }

        s.close();
        return ttList;
    }



    /**
     * Estimates the number of block accesses to compute the join.
     * The formula is:
     * <pre> B(hashjoin(p1,p2)) = 3 * (B(p1) + B(p2))</pre>
     *
     * @see simpledb.plan.Plan#blocksAccessed()
     */
    public int blocksAccessed() {
        return 3 * (smallPlan.blocksAccessed() + largePlan.blocksAccessed());
    }

    /**
     * Estimates the number of output records in the join.
     * The formula is:
     * <pre> R(hashjoin(p1,p2)) = R(p1)*R(p2)/max{V(p1,F1),V(p2,F2)} </pre>
     *
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
     *
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
     *
     * @see simpledb.plan.Plan#schema()
     */
    public Schema schema() {
        return sch;
    }

    public String toString() {
        return String.format("(%s) hash join (%s)", smallPlan.toString(), largePlan.toString());
    }
}

