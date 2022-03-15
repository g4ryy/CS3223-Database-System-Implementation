package simpledb.materialize;

import java.util.*;

import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.plan.Plan;
import simpledb.query.*;

/**
 * The Plan class for the <i>sort</i> operator.
 * @author Edward Sciore
 */
public class SortPlan implements Plan {
   private Transaction tx;
   private Plan p;
   private Schema sch;
   private RecordComparator comp;
   private boolean isDistinct;
   private List<String> selectFields;
   private List<OrderField> sortFields;
   private int numOfPasses;
   
   /**
    * Create a sort plan for the specified query.
    * @param p the plan for the underlying query
    * @param sortfields the fields to sort by
    * @param tx the calling transaction
    * @param isDistinct a boolean value indicating if distinct tuples have to be returned
    */
   public SortPlan(Transaction tx, Plan p, List<OrderField> sortfields, boolean isDistinct) {
	      this.tx = tx;
	      this.p = p;
	      sch = p.schema();
	      comp = new RecordComparator(sortfields);
	      this.isDistinct = isDistinct;
	      this.selectFields = new ArrayList<>();
	      this.sortFields = sortfields;
	      numOfPasses = 0;
   }
   
   /**
    * Create a sort plan for the specified query.
    * @param p the plan for the underlying query
    * @param sortfields the fields to sort by
    * @param tx the calling transaction
    * @param isDistinct a boolean value indicating if distinct tuples have to be returned
    * @param selectFields a list of field names appearing in the select clause of the query
    */
   public SortPlan(Transaction tx, Plan p, List<OrderField> sortfields, boolean isDistinct, List<String> selectFields) {
      this.tx = tx;
      this.p = p;
      sch = p.schema();
      comp = new RecordComparator(sortfields);
      this.isDistinct = isDistinct;
      this.selectFields = selectFields;
      this.sortFields = sortfields;
      numOfPasses = 0;
   }

   /**
    * This method is where most of the action is.
    * Up to 2 sorted temporary tables are created,
    * and are passed into SortScan for final merging.
    * @see simpledb.plan.Plan#open()
    */
   public Scan open() {
      Scan src = p.open();
      List<TempTable> runs = splitIntoRuns(src);
      numOfPasses += 1;
      src.close();
      if (runs.size() == 1 && isDistinct) {
    	  // To remove duplicates
    	  runs = mergeSingleRunWithEmptyRun(runs);
    	  numOfPasses += 1;
      }
      while (runs.size() > 1) {
    	  runs = doAMergeIteration(runs);
    	  numOfPasses += 1;
      }
      return new SortScan(runs, comp);
   }
   
   /**
    * Return the number of blocks in the sorted table,
    * which is the same as it would be in a
    * materialized table.
    * It does <i>not</i> include the one-time cost
    * of materializing and sorting the records.
    * @see simpledb.plan.Plan#blocksAccessed()
    */
   public int blocksAccessed() {
      // does not include the one-time cost of sorting
      Plan mp = new MaterializePlan(tx, p); // not opened; just for analysis
      return mp.blocksAccessed();
   }
   
   /**
    * Return the number of records in the sorted table,
    * which is the same as in the underlying query.
    * @see simpledb.plan.Plan#recordsOutput()
    */
   public int recordsOutput() {
      return p.recordsOutput();
   }
   
   /**
    * Return the number of distinct field values in
    * the sorted table, which is the same as in
    * the underlying query.
    * @see simpledb.plan.Plan#distinctValues(java.lang.String)
    */
   public int distinctValues(String fldname) {
      return p.distinctValues(fldname);
   }
   
   /**
    * Return the schema of the sorted table, which
    * is the same as in the underlying query.
    * @see simpledb.plan.Plan#schema()
    */
   public Schema schema() {
      return sch;
   }
   
   private List<TempTable> splitIntoRuns(Scan src) {
      List<TempTable> temps = new ArrayList<>();
      src.beforeFirst();
      if (!src.next())
         return temps;
      TempTable currenttemp = new TempTable(tx, sch);
      temps.add(currenttemp);
      UpdateScan currentscan = currenttemp.open();
      while (copy(src, currentscan))
         if (comp.compare(src, currentscan) < 0) {
         // start a new run
         currentscan.close();
         currenttemp = new TempTable(tx, sch);
         temps.add(currenttemp);
         currentscan = (UpdateScan) currenttemp.open();
      }
      currentscan.close();
      return temps;
   }
   
   private List<TempTable> doAMergeIteration(List<TempTable> runs) {
      List<TempTable> result = new ArrayList<>();
      while (runs.size() > 1) {
         TempTable p1 = runs.remove(0);
         TempTable p2 = runs.remove(0);
         result.add(mergeTwoRuns(p1, p2));
      }
      if (runs.size() == 1) {
    	  result.add(runs.get(0));
      }
      return result;
   }
   
   private List<TempTable> mergeSingleRunWithEmptyRun(List<TempTable> runs) {
	   List<TempTable> result = new ArrayList<>();
	   TempTable p = runs.get(0);
	   TempTable empty = new TempTable(tx, sch);
	   // Merging a single run with an empty run helps to remove duplicates from the run
	   result.add(mergeTwoRuns(p, empty));
	   return result;
   }
   
   private TempTable mergeTwoRuns(TempTable p1, TempTable p2) {
      TableScan src1 = (TableScan) p1.open();
      TableScan src2 = (TableScan) p2.open();
      TempTable result = new TempTable(tx, sch);
      UpdateScan dest = result.open();
      List<Constant> prev = null;
      
      boolean hasmore1 = src1.next();
      boolean hasmore2 = src2.next();
      while (hasmore1 && hasmore2) {
    	  if (isDistinct && prev != null) {
     		 if (prev.equals(src1.getValuesForFields(selectFields))) {
     			 hasmore1 = src1.next();
     			 continue;
     		 }
     		 if (prev.equals(src2.getValuesForFields(selectFields))) {
     			 hasmore2 = src2.next();
     			 continue;
     		 }
     	 }
     	  
          if (comp.compare(src1, src2) < 0) {
        	 if (isDistinct) {
        		 prev = src1.getValuesForFields(selectFields);
        	 }
         	 hasmore1 = copy(src1, dest);
          } else {
        	 if (isDistinct) {
        		 prev = src2.getValuesForFields(selectFields);
        	 }
         	 hasmore2 = copy(src2, dest);
          }
      }
      
      if (hasmore1) {
    	  while (hasmore1) {
         	 if (isDistinct && prev != null) {
          		 if (prev.equals(src1.getValuesForFields(selectFields))) {
          			 hasmore1 = src1.next();
          			 continue;
          		 }
          	 }
         	 if (isDistinct) {
         		prev = src1.getValuesForFields(selectFields);
         	 }
             hasmore1 = copy(src1, dest);
          }
      } else {
         while (hasmore2) {
        	 if (isDistinct && prev != null) {
         		 if (prev.equals(src2.getValuesForFields(selectFields))) {
         			 hasmore2 = src2.next();
         			 continue;
         		 }
         	 }
        	 if (isDistinct) {
        		 prev = src2.getValuesForFields(selectFields);
        	 }
             hasmore2 = copy(src2, dest);
         }
      }
      src1.close();
      src2.close();
      dest.close();
      return result;
   }
   
   private boolean copy(Scan src, UpdateScan dest) {
      dest.insert();
      for (String fldname : sch.fields())
         dest.setVal(fldname, src.getVal(fldname));
      return src.next();
   }
   
   public int getNumOfPasses() {
	   return numOfPasses;
   }
   
   public String toString() {
	   String sortFieldsStr = "";
	   for (OrderField sortField : sortFields) {
		   sortFieldsStr += String.format("%s %s, ", sortField.getField(), sortField.getType());
	   }
	   sortFieldsStr = sortFieldsStr.substring(0, sortFieldsStr.length() - 2);
	   if (!isDistinct) {
		   return String.format("sort(%s)[%s]", sortFieldsStr, p.toString());
	   }
	   String selectFieldsStr = "";
	   for (String selectField : selectFields) {
		   selectFieldsStr += selectField + ", ";
	   }
	   selectFieldsStr = selectFieldsStr.substring(0, selectFieldsStr.length() - 2);
	   return String.format("distinct(%s)[sort(%s)[%s]]", selectFieldsStr, sortFieldsStr, p.toString());
   }
}
