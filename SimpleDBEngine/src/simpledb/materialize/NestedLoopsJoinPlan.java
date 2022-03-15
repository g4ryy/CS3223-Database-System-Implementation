package simpledb.materialize;

import simpledb.tx.Transaction;
import simpledb.plan.Plan;
import simpledb.query.*;
import simpledb.record.*;

import java.util.*;
import java.util.stream.Collectors;

/**
 * The Plan class for the <i>mergejoin</i> operator.
 * @author Edward Sciore
 */
public class NestedLoopsJoinPlan implements Plan {
   private Plan p1, p2;
   private String fldname1, fldname2;
   private Schema sch = new Schema();
   private Operator opr;
   
   /**
    * Creates a mergejoin plan for the two specified queries.
    * The RHS must be materialized after it is sorted, 
    * in order to deal with possible duplicates.
    * @param p1 the LHS query plan
    * @param p2 the RHS query plan
    * @param fldname1 the LHS join field
    * @param fldname2 the RHS join field
    * @param tx the calling transaction
    */
   public NestedLoopsJoinPlan(Transaction tx, Plan p1, Plan p2, String fldname1, String fldname2, Operator opr) {
      this.fldname1 = fldname1;
      this.fldname2 = fldname2;
      this.p1 = p1;
      this.p2 = p2;
      sch.addAll(p1.schema());
      sch.addAll(p2.schema());
      this.opr = opr;
   }
   
   /** The method first sorts its two underlying scans
     * on their join field. It then returns a mergejoin scan
     * of the two sorted table scans.
     * @see simpledb.plan.Plan#open()
     */
   public Scan open() {
      Scan s1 = p1.open();
      Scan s2 = p2.open();
      return new NestedLoopsJoinScan(s1, s2, fldname1, fldname2, opr);
   }
   
   /**
    * Return the number of block accesses required to
    * mergejoin the sorted tables.
    * Since a mergejoin can be performed with a single
    * pass through each table, the method returns
    * the sum of the block accesses of the 
    * materialized sorted tables.
    * It includes the one-time cost
    * of materializing and sorting the records.
    * @see simpledb.plan.Plan#blocksAccessed()
    */
   public int blocksAccessed() {
	   int numOfPassesRequiredToSortP1 = ((SortPlan) p1).getNumOfPasses();
	   int numOfPassesRequiredToSortP2 = ((SortPlan) p2).getNumOfPasses();
	   return 2 * p1.blocksAccessed() * numOfPassesRequiredToSortP1 
			   + 2 * p2.blocksAccessed() * numOfPassesRequiredToSortP2
			   + p1.blocksAccessed() + p2.blocksAccessed();
   }
   
   /**
    * Return the number of records in the join.
    * Assuming uniform distribution, the formula is:
    * <pre> R(join(p1,p2)) = R(p1)*R(p2)/max{V(p1,F1),V(p2,F2)}</pre>
    * @see simpledb.plan.Plan#recordsOutput()
    */
   public int recordsOutput() {
      int maxvals = Math.max(p1.distinctValues(fldname1),
                             p2.distinctValues(fldname2));
      return (p1.recordsOutput() * p2.recordsOutput()) / maxvals;
   }
   
   /**
    * Estimate the distinct number of field values in the join.
    * Since the join does not increase or decrease field values,
    * the estimate is the same as in the appropriate underlying query.
    * @see simpledb.plan.Plan#distinctValues(java.lang.String)
    */
   public int distinctValues(String fldname) {
      if (p1.schema().hasField(fldname))
         return p1.distinctValues(fldname);
      else
         return p2.distinctValues(fldname);
   }
   
   /**
    * Return the schema of the join,
    * which is the union of the schemas of the underlying queries.
    * @see simpledb.plan.Plan#schema()
    */
   public Schema schema() {
      return sch;
   }
   
   public String toString() {
	   return String.format("(%s) nested loops join (%s)", p1.toString(), p2.toString());
   }
}

