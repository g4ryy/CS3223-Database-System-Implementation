package simpledb.materialize;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import simpledb.query.*;
import simpledb.record.Schema;

/**
 * The Scan class for the <i>hashjoin</i> operator.
 */
public class HashJoinScan implements Scan {
    private Map<Integer, TempTable> smallPartitions;
    private Map<Integer, TempTable> largePartitions;
    private Schema smallSchema;
    private String smallField;
    private String largeField;
    private Scan largeScan;
    private int smallScanIndex;
    private int currPartition;
    private Queue<Integer> partitionQueue = new LinkedList<>();
    private Map<Constant, List<Map<String, Constant>>> map;

    /**
     * Create a hashjoin scan for the two partitioned scans.
     *
     * @param smallPartitions the smaller partitioned scan
     * @param largePartitions the larger partitioned scan
     * @param smallField the join field of the smaller partitioned table
     * @param largeField the join field of the larger partitioned table
     * @param schema the schema of the smaller scan
     */
    public HashJoinScan(Map<Integer, TempTable> smallPartitions, Map<Integer, TempTable> largePartitions,
                        String smallField, String largeField, Schema schema) {
        this.smallPartitions = smallPartitions;
        this.largePartitions = largePartitions;
        this.smallField = smallField;
        this.largeField = largeField;
        smallPartitions.keySet().forEach(p -> partitionQueue.offer(p));
        this.smallSchema = schema;
        beforeFirst();
    }

    /**
     * Close the scan by closing the larger partitioned scan.
     * @see simpledb.query.Scan#close()
     */
    public void close() {
        if (largeScan != null) {
            largeScan.close();
        }
    }

    private boolean nextPartition() {
        if (partitionQueue.isEmpty()) {
            return false;
        }

        currPartition = partitionQueue.poll();

        Scan smallScan = smallPartitions.get(currPartition).open();
        smallScanIndex = -1;
        smallScan.beforeFirst();

        if (largePartitions.containsKey(currPartition)) {
            largeScan = largePartitions.get(currPartition).open();
        } else {
            return nextPartition();
        }

        largeScan.beforeFirst();

        if (!largeScan.next()) {
            return nextPartition();
        }

        map = new HashMap<>();

        while (smallScan.next()) {
            Constant val = smallScan.getVal(smallField);
            if (!map.containsKey(val)) {
                map.put(val, new ArrayList<>());
            }

            Map<String, Constant> tmp = new HashMap<>();
            for (String field : smallSchema.fields()) {
                tmp.put(field, smallScan.getVal(field));
            }
            map.get(val).add(tmp);
        }
        smallScan.close();

        return true;
    }

    /**
     * Moves to the first partition
     * @see simpledb.query.Scan#beforeFirst()
     */
    public void beforeFirst() {
        nextPartition();
    }

    /**
     * @see simpledb.query.Scan#next()
     */
    public boolean next() {
        // largeScan == null indicates that the smaller scan is empty, so just return false
        if (largeScan != null) {
            Constant val = largeScan.getVal(largeField);
            if (map.containsKey(val) &&
                smallScanIndex < map.get(val).size() - 1) {
                smallScanIndex++;
                return true;
            }

            while (largeScan.next()) {
                if (map.containsKey(largeScan.getVal(largeField))) {
                    smallScanIndex = -1;
                    return next();
                }
            }

            if (nextPartition()) {
                return next();
            }
        }

        return false;
    }

    /**
     * Return the integer value of the specified field.
     * The value is obtained from whichever scan
     * contains the field.
     * @see simpledb.query.Scan#getInt(java.lang.String)
     */
    public int getInt(String fldname) {
        if (largeScan.hasField(fldname))
            return largeScan.getInt(fldname);
        else
            return map.get(largeScan.getVal(largeField)).get(smallScanIndex).get(fldname).asInt();
    }

    /**
     * Return the string value of the specified field.
     * The value is obtained from whichever scan
     * contains the field.
     * @see simpledb.query.Scan#getString(java.lang.String)
     */
    public String getString(String fldname) {
        if (largeScan.hasField(fldname))
            return largeScan.getString(fldname);
        else
            return map.get(largeScan.getVal(largeField)).get(smallScanIndex).get(fldname).asString();
    }

    /**
     * Return the value of the specified field.
     * The value is obtained from whichever scan
     * contains the field.
     * @see simpledb.query.Scan#getVal(java.lang.String)
     */
    public Constant getVal(String fldname) {
        if (largeScan.hasField(fldname))
            return largeScan.getVal(fldname);
        else
            return map.get(largeScan.getVal(largeField)).get(smallScanIndex).get(fldname);
    }

    /**
     * Return true if the specified field is in
     * either of the underlying scans.
     * @see simpledb.query.Scan#hasField(java.lang.String)
     */
    public boolean hasField(String fldname) {
        return largeScan.hasField(fldname) ||
                map.get(largeScan.getVal(largeField)).get(smallScanIndex).containsKey((fldname));
    }
}

