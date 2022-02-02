package simpledb.query;

/**
 * An operator is used to compare 2 expressions in a term.
 */
public class Operator {
    private final String operator;

    /**
     * Create a new operator that will be used to compare 2 expressions in a term based on the
     * given string representation.
     *
     * @param operator String representation of the operator
     */
    public Operator(String operator) {
        this.operator = operator;
    }

    /**
     * Executes the comparison between the 2 inputs
     *
     * @param first The expression on the lhs of the term
     * @param second The expression on the rhs of the term
     */
    public boolean evaluate(Constant first, Constant second) {
        switch (operator) {
        case "<>":
        case "!=":
            return !first.equals(second);
        case "<":
            return first.compareTo(second) < 0;
        case "<=":
            return first.compareTo(second) <= 0;
        case ">":
            return first.compareTo(second) > 0;
        case ">=":
            return first.compareTo(second) >= 0;
        default:
            return first.equals(second);
        }
    }
}
