package simpledb.parse;

import java.util.*;

import simpledb.materialize.AggregationFn;
import simpledb.query.*;

/**
 * Data for the SQL <i>select</i> statement.
 * @author Edward Sciore
 */
public class QueryData {
   private List<String> fields;
   private Collection<String> tables;
   private Predicate pred;
   private List<OrderField> orderFields;
   private List<AggregationFn> aggFields;
   private List<String> groupByFields;
   private boolean isDistinct;

   /**
    * Saves the field, table list, predicate, ordering fields and aggregation fields.
    */
   public QueryData(List<String> fields, Collection<String> tables,
                    Predicate pred, List<OrderField> orderFields,
                    List<AggregationFn> aggFields, List<String> groupByFields,
                    boolean isDistinct) {
      this.fields = fields;
      this.tables = tables;
      this.pred = pred;
      this.orderFields = orderFields;
      this.aggFields = aggFields;
      this.groupByFields = groupByFields;
      for (String field : groupByFields) {
         if (!fields.contains(field)) {
            fields.add(field);
         }
      }
      this.isDistinct = isDistinct;
   }
   
   /**
    * Returns the fields mentioned in the select clause.
    * @return a list of field names
    */
   public List<String> fields() {
      return fields;
   }
   
   /**
    * Returns the tables mentioned in the from clause.
    * @return a collection of table names
    */
   public Collection<String> tables() {
      return tables;
   }
   
   /**
    * Returns the predicate that describes which
    * records should be in the output table.
    * @return the query predicate
    */
   public Predicate pred() {
      return pred;
   }

   /**
    * Returns the list of fields to order by
    *
    * @return a list of (field, ordering type) pairs
    */
   public List<OrderField> orderFields() {
      return orderFields;
   }

   /**
    * Returns the list of aggregation functions
    *
    * @return a list of aggregation functions
    */
   public List<AggregationFn> aggFields() {
      return aggFields;
   }

   /**
    * Returns the list of group by fields
    * @return a list of group by fields
    */
   public List<String> groupByFields() {
      return groupByFields;
   }
   
   public boolean isDistinct() {
	   return isDistinct;
   }
   
   public String toString() {
      String result = "select ";
      for (String fldname : fields)
         result += fldname + ", ";
      result = result.substring(0, result.length()-2); //remove final comma
      result += " from ";
      for (String tblname : tables)
         result += tblname + ", ";
      result = result.substring(0, result.length()-2); //remove final comma
      String predstring = pred.toString();
      if (!predstring.equals(""))
         result += " where " + predstring;
      if (orderFields.size() > 0) {
         result += " order by ";
         for (OrderField orderField : orderFields) {
            result += String.format("%s %s, ", orderField.getField(), orderField.getType());
         }
         result = result.substring(0, result.length()-2); //remove final comma
      }
      return result;
   }
}
