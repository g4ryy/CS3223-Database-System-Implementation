package simpledb.plan;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import simpledb.materialize.AggregationFn;
import simpledb.materialize.GroupByPlan;
import simpledb.materialize.SortPlan;
import simpledb.metadata.MetadataMgr;
import simpledb.parse.Parser;
import simpledb.parse.QueryData;
import simpledb.tx.Transaction;

/**
 * The simplest, most naive query planner possible.
 *
 * @author Edward Sciore
 */
public class BasicQueryPlanner implements QueryPlanner {
    private MetadataMgr mdm;

    public BasicQueryPlanner(MetadataMgr mdm) {
        this.mdm = mdm;
    }

    /**
     * Creates a query plan as follows.  It first takes
     * the product of all tables and views; it then selects on the predicate;
     * It then performs group by or aggregation if needed, followed by sorting if needed,
     * and finally it projects on the field list.
     */
    public Plan createPlan(QueryData data, Transaction tx) {
        // Step 1: Create a plan for each mentioned table or view.
        List<Plan> plans = new ArrayList<>();
        for (String tblname : data.tables()) {
            String viewdef = mdm.getViewDef(tblname, tx);
            if (viewdef != null) { // Recursively plan the view.
                Parser parser = new Parser(viewdef);
                QueryData viewdata = parser.query();
                plans.add(createPlan(viewdata, tx));
            } else
                plans.add(new TablePlan(tx, tblname, mdm));
        }

        // Step 2: Create the product of all table plans
        Plan p = plans.remove(0);
        for (Plan nextplan : plans)
            p = new ProductPlan(p, nextplan);

        // Step 3: Add a selection plan for the predicate
        p = new SelectPlan(p, data.pred());

        // Step 4: Group by and aggregate if needed
        if (!data.groupByFields().isEmpty() || !data.aggFields().isEmpty()) {
            p = new GroupByPlan(tx, p, data.groupByFields(), data.aggFields());
        }

        // Step 5: Sort by field names and specified ordering, remove duplicates if requested
        if (!data.orderFields().isEmpty()) {
            p = new SortPlan(tx, p, data.orderFields(), data.isDistinct(), data.fields());
        }

        data.fields().addAll(data.aggFields().stream().map(AggregationFn::fieldName).collect(Collectors.toList()));

        // Step 6: Project on the field names
        p = new ProjectPlan(p, data.fields());

        // Display the query plan
        System.out.println("Query Plan:");
        System.out.println(p);

        return p;
    }
}
