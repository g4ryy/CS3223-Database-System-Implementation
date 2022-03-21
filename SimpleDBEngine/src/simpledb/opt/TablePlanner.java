package simpledb.opt;

import java.util.Map;
import java.util.PriorityQueue;

import simpledb.index.planner.IndexJoinPlan;
import simpledb.index.planner.IndexSelectPlan;
import simpledb.materialize.HashJoinPlan;
import simpledb.materialize.MergeJoinPlan;
import simpledb.materialize.NestedLoopsJoinPlan;
import simpledb.metadata.IndexInfo;
import simpledb.metadata.MetadataMgr;
import simpledb.multibuffer.MultibufferProductPlan;
import simpledb.plan.Plan;
import simpledb.plan.SelectPlan;
import simpledb.plan.TablePlan;
import simpledb.query.Constant;
import simpledb.query.Expression;
import simpledb.query.Operator;
import simpledb.query.Predicate;
import simpledb.query.Term;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

/**
 * This class contains methods for planning a single table.
 *
 * @author Edward Sciore
 */
class TablePlanner {
    private TablePlan myplan;
    private Predicate mypred;
    private Schema myschema;
    private Map<String, IndexInfo> indexes;
    private Transaction tx;

    /**
     * Creates a new table planner.
     * The specified predicate applies to the entire query.
     * The table planner is responsible for determining
     * which portion of the predicate is useful to the table,
     * and when indexes are useful.
     *
     * @param tblname the name of the table
     * @param mypred  the query predicate
     * @param tx      the calling transaction
     */
    public TablePlanner(String tblname, Predicate mypred, Transaction tx, MetadataMgr mdm) {
        this.mypred = mypred;
        this.tx = tx;
        myplan = new TablePlan(tx, tblname, mdm);
        myschema = myplan.schema();
        indexes = mdm.getIndexInfo(tblname, tx);
    }

    /**
     * Constructs a select plan for the table.
     * The plan will use an indexselect, if possible.
     *
     * @return a select plan for the table.
     */
    public Plan makeSelectPlan() {
        Predicate selectpred = mypred.selectSubPred(myschema);
        if (selectpred != null) {
            for (Term term : selectpred.getTerms()) {
                Expression lhs = term.getLhs();
                Expression rhs = term.getRhs();
                Operator operator = term.getOperator();
                if (indexes.keySet().contains(lhs.asFieldName()) || indexes.keySet().contains(rhs.asFieldName())) {
                    if (!operator.toString().equals("=")) {
                        // Don't use index for non-equi selects on an indexed field
                        return addSelectPred(myplan);
                    }
                }
            }
        }
        Plan p = makeIndexSelect();
        if (p == null)
            p = myplan;
        return addSelectPred(p);
    }

    /**
     * Constructs a join plan of the specified plan
     * and the table.  The plan will use the join with the lowest cost.
     * The method returns null if no join is possible.
     *
     * @param current the specified plan
     * @return a join plan of the plan and this table
     */
    public Plan makeJoinPlan(Plan current) {
        Schema currsch = current.schema();
        Predicate joinpred = mypred.joinSubPred(myschema, currsch);
        if (joinpred == null)
            return null;

        PriorityQueue<Plan> pq = new PriorityQueue<>((x, y) -> x.blocksAccessed() - y.blocksAccessed());
        Plan tempPlan = makeIndexJoin(current, currsch);
        if (tempPlan != null) {
            pq.add(tempPlan);
        }
        tempPlan = makeMergeJoin(current, currsch, joinpred);
        if (tempPlan != null) {
            pq.add(tempPlan);

        }
        tempPlan = makeHashJoin(current, currsch, joinpred);
        if (tempPlan != null) {
            pq.add(tempPlan);
        }
        tempPlan = makeNestedLoopsJoin(current, currsch, joinpred);
        if (tempPlan != null) {
            pq.add(tempPlan);
        }

        if (pq.size() == 0) {
            return makeProductJoin(current, currsch);
        }

        return pq.poll();
    }

    /**
     * Constructs a product plan of the specified plan and
     * this table.
     *
     * @param current the specified plan
     * @return a product plan of the specified plan and this table
     */
    public Plan makeProductPlan(Plan current) {
        Plan p = addSelectPred(myplan);
        return new MultibufferProductPlan(tx, current, p);
    }

    private Plan makeIndexSelect() {
        for (String fldname : indexes.keySet()) {
            Constant val = mypred.equatesWithConstant(fldname);
            if (val != null) {
                IndexInfo ii = indexes.get(fldname);
                return new IndexSelectPlan(myplan, ii, val);
            }
        }
        return null;
    }

    private Plan makeMergeJoin(Plan current, Schema currsch, Predicate joinPred) {
        for (String fldname : myschema.fields()) {
            String matchField = joinPred.equatesWithField(fldname);
            Operator opr = joinPred.getOprForField(fldname);

            if (matchField != null && opr.toString().equals("=") && currsch.hasField(matchField)) {
                Plan p = new MergeJoinPlan(tx, myplan, current, fldname, matchField);
                p = addSelectPred(p);
                return addJoinPred(p, currsch);
            }
        }
        return null;
    }

    private Plan makeHashJoin(Plan current, Schema currsch, Predicate joinPred) {
        for (String fldName : myschema.fields()) {
            String matchField = joinPred.equatesWithField(fldName);
            Operator opr = joinPred.getOprForField(fldName);

            if (matchField != null && opr.toString().equals("=") && currsch.hasField(matchField)) {
                Plan p = new HashJoinPlan(tx, myplan, current, fldName, matchField);
                p = addSelectPred(p);
                return addJoinPred(p, currsch);
            }
        }
        return null;
    }

    private Plan makeIndexJoin(Plan current, Schema currsch) {
        for (String fldname : indexes.keySet()) {
            String outerfield = mypred.equatesWithField(fldname);
            Operator opr = mypred.getOprForField(fldname);
            if (outerfield != null && opr.toString().equals("=") && currsch.hasField(outerfield)) {
                IndexInfo ii = indexes.get(fldname);
                Plan p = new IndexJoinPlan(current, myplan, ii, outerfield);
                p = addSelectPred(p);
                return addJoinPred(p, currsch);
            }
        }
        return null;
    }

    private Plan makeNestedLoopsJoin(Plan current, Schema currsch, Predicate joinPred) {
        for (String fldName : myschema.fields()) {
            String matchField = joinPred.equatesWithField(fldName);
            Operator opr = joinPred.getOprForField(fldName);

            if (matchField != null && currsch.hasField(matchField)) {
                Plan p;
                if (currsch.hasField(joinPred.getTerms().get(0).getLhs().asFieldName())) {
                    p = new NestedLoopsJoinPlan(tx, current, myplan, matchField, fldName, opr);
                } else {
                    p = new NestedLoopsJoinPlan(tx, myplan, current, fldName, matchField, opr);
                }
                p = addSelectPred(p);
                return addJoinPred(p, currsch);
            }
        }
        return null;
    }

    private Plan makeProductJoin(Plan current, Schema currsch) {
        Plan p = makeProductPlan(current);
        return addJoinPred(p, currsch);
    }

    private Plan addSelectPred(Plan p) {
        Predicate selectpred = mypred.selectSubPred(myschema);
        if (selectpred != null)
            return new SelectPlan(p, selectpred);
        else
            return p;
    }

    private Plan addJoinPred(Plan p, Schema currsch) {
        Predicate joinpred = mypred.joinSubPred(currsch, myschema);
        if (joinpred != null)
            return new SelectPlan(p, joinpred);
        else
            return p;
    }
}
