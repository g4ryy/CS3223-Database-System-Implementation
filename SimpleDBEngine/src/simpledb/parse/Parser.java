package simpledb.parse;

import java.util.*;

import simpledb.materialize.AggregationFn;
import simpledb.materialize.AvgFn;
import simpledb.materialize.CountFn;
import simpledb.materialize.MaxFn;
import simpledb.materialize.MinFn;
import simpledb.materialize.SumFn;
import simpledb.query.*;
import simpledb.record.*;

/**
 * The SimpleDB parser.
 * @author Edward Sciore
 */
public class Parser {
   private Lexer lex;
   
   public Parser(String s) {
      lex = new Lexer(s);
   }
   
// Methods for parsing predicates, terms, expressions, constants, and fields
   
   public String field() {
      return lex.eatId();
   }
   
   public Constant constant() {
      if (lex.matchStringConstant())
         return new Constant(lex.eatStringConstant());
      else
         return new Constant(lex.eatIntConstant());
   }
   
   public Expression expression() {
      if (lex.matchId())
         return new Expression(field());
      else
         return new Expression(constant());
   }
   
   public Term term() {
      Expression lhs = expression();
      Operator operator = lex.eatOperator();
      //lex.eatDelim('=');
      Expression rhs = expression();
      return new Term(lhs, rhs, operator);
   }
   
   public Predicate predicate() {
      Predicate pred = new Predicate(term());
      if (lex.matchKeyword("and")) {
         lex.eatKeyword("and");
         pred.conjoinWith(predicate());
      }
      return pred;
   }
   
// Methods for parsing queries
   
   public QueryData query() {
      lex.eatKeyword("select");
      List<String> fields = new ArrayList<>();
      List<AggregationFn> aggFields = new ArrayList<>();

      while (true) {
         if (lex.matchAggregate()) {
            aggFields.add(getAggregate(fields));
         } else {
            fields.add(field());
         }

         if (!lex.matchDelim(',')) {
            break;
         }
         lex.eatDelim(',');
      }

      lex.eatKeyword("from");
      Collection<String> tables = tableList();
      Predicate pred = new Predicate();
      if (lex.matchKeyword("where")) {
         lex.eatKeyword("where");
         pred = predicate();
      }

      List<String> groupByFields = new ArrayList<>();
      if (lex.matchKeyword("group")) {
         lex.eatKeyword("group");
         lex.eatKeyword("by");
         groupByFields = groupByList();
      }

      List<OrderField> orderFields = new ArrayList<>();
      if (lex.matchKeyword("order")) {
         lex.eatKeyword("order");
         lex.eatKeyword("by");

         orderFields.add(new OrderField(field(), orderType()));

         while (lex.matchDelim(',')) {
            lex.eatDelim(',');
            orderFields.add(new OrderField(field(), orderType()));
         }
      }

      return new QueryData(fields, tables, pred, orderFields, aggFields, groupByFields);
   }

   private String orderType() {
      String type = "asc";

      // if the token after the field is not a word, then by default it's asc
      if (!lex.matchId() && !lex.matchKeyword("asc") && !lex.matchKeyword("desc")) {
         return type;
      }

      if (lex.matchKeyword("desc")) {
         type = "desc";
      }

      lex.eatKeyword(type);
      return type;
   }

   private AggregationFn getAggregate(List<String> fields) {
      String function = lex.eatAggregate();
      lex.eatDelim('(');
      String field = field();
      fields.add(field);
      lex.eatDelim(')');

      switch (function) {
      case "sum":
         return new SumFn(field);
      case "count":
         return new CountFn(field);
      case "avg":
         return new AvgFn(field);
      case "min":
         return new MinFn(field);
      case "max":
         return new MaxFn(field);
      default:
         return null;
      }
   }

   private List<String> selectList() {
      List<String> L = new ArrayList<String>();
      L.add(field());
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         L.addAll(selectList());
      }
      return L;
   }
   
   private Collection<String> tableList() {
      Collection<String> L = new ArrayList<String>();
      L.add(lex.eatId());
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         L.addAll(tableList());
      }
      return L;
   }

   private List<String> groupByList() {
      List<String> groupByFields = new ArrayList<>();
      groupByFields.add(field());

      while (lex.matchDelim(',')) {
         lex.eatDelim(',');
         groupByFields.add(field());
      }
      return groupByFields;
   }
   
// Methods for parsing the various update commands
   
   public Object updateCmd() {
      if (lex.matchKeyword("insert"))
         return insert();
      else if (lex.matchKeyword("delete"))
         return delete();
      else if (lex.matchKeyword("update"))
         return modify();
      else
         return create();
   }
   
   private Object create() {
      lex.eatKeyword("create");
      if (lex.matchKeyword("table"))
         return createTable();
      else if (lex.matchKeyword("view"))
         return createView();
      else
         return createIndex();
   }
   
// Method for parsing delete commands
   
   public DeleteData delete() {
      lex.eatKeyword("delete");
      lex.eatKeyword("from");
      String tblname = lex.eatId();
      Predicate pred = new Predicate();
      if (lex.matchKeyword("where")) {
         lex.eatKeyword("where");
         pred = predicate();
      }
      return new DeleteData(tblname, pred);
   }
   
// Methods for parsing insert commands
   
   public InsertData insert() {
      lex.eatKeyword("insert");
      lex.eatKeyword("into");
      String tblname = lex.eatId();
      lex.eatDelim('(');
      List<String> flds = fieldList();
      lex.eatDelim(')');
      lex.eatKeyword("values");
      lex.eatDelim('(');
      List<Constant> vals = constList();
      lex.eatDelim(')');
      return new InsertData(tblname, flds, vals);
   }
   
   private List<String> fieldList() {
      List<String> L = new ArrayList<String>();
      L.add(field());
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         L.addAll(fieldList());
      }
      return L;
   }
   
   private List<Constant> constList() {
      List<Constant> L = new ArrayList<Constant>();
      L.add(constant());
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         L.addAll(constList());
      }
      return L;
   }
   
// Method for parsing modify commands
   
   public ModifyData modify() {
      lex.eatKeyword("update");
      String tblname = lex.eatId();
      lex.eatKeyword("set");
      String fldname = field();
      lex.eatDelim('=');
      Expression newval = expression();
      Predicate pred = new Predicate();
      if (lex.matchKeyword("where")) {
         lex.eatKeyword("where");
         pred = predicate();
      }
      return new ModifyData(tblname, fldname, newval, pred);
   }
   
// Method for parsing create table commands
   
   public CreateTableData createTable() {
      lex.eatKeyword("table");
      String tblname = lex.eatId();
      lex.eatDelim('(');
      Schema sch = fieldDefs();
      lex.eatDelim(')');
      return new CreateTableData(tblname, sch);
   }
   
   private Schema fieldDefs() {
      Schema schema = fieldDef();
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         Schema schema2 = fieldDefs();
         schema.addAll(schema2);
      }
      return schema;
   }
   
   private Schema fieldDef() {
      String fldname = field();
      return fieldType(fldname);
   }
   
   private Schema fieldType(String fldname) {
      Schema schema = new Schema();
      if (lex.matchKeyword("int")) {
         lex.eatKeyword("int");
         schema.addIntField(fldname);
      }
      else {
         lex.eatKeyword("varchar");
         lex.eatDelim('(');
         int strLen = lex.eatIntConstant();
         lex.eatDelim(')');
         schema.addStringField(fldname, strLen);
      }
      return schema;
   }
   
// Method for parsing create view commands
   
   public CreateViewData createView() {
      lex.eatKeyword("view");
      String viewname = lex.eatId();
      lex.eatKeyword("as");
      QueryData qd = query();
      return new CreateViewData(viewname, qd);
   }
   
   
//  Method for parsing create index commands
   
   public CreateIndexData createIndex() {
      lex.eatKeyword("index");
      String idxname = lex.eatId();
      lex.eatKeyword("on");
      String tblname = lex.eatId();
      lex.eatDelim('(');
      String fldname = field();
      lex.eatDelim(')');
      lex.eatKeyword("using");
      String idxType = "hash";
      if (lex.matchKeyword("btree")) {
         idxType = "btree";
      }
      lex.eatKeyword(idxType);

      return new CreateIndexData(idxname, tblname, fldname, idxType);
   }
}

