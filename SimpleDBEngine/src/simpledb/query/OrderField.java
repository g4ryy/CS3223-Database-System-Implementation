package simpledb.query;

/**
 * Represents a pair of sort field and the corresponding ordering type,
 * where the field is the field to order by and ordering type is either asc or desc
 */
public class OrderField {
    private final String field;
    private final String type;

    public OrderField(String field, String type) {
        this.field = field;
        this.type = type;
    }

    public String getField() {
        return this.field;
    }

    public String getType() {
        return this.type;
    }

    public String toString() {
        return String.format("%s %s", field, type);
    }
}

