package simpledb.materialize;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import javax.sound.sampled.Line;

import simpledb.query.Constant;
import simpledb.query.Scan;

/*
 * The Scan class for the <i>hashjoin</i> operator.
 */
public class HashJoinScan implements Scan {
    private String smallField;
    private String largeField;

    private int numPartitions;
    private Map<Integer, TempTable> smallPartitions;
    private Map<Integer, TempTable> largePartitions;

    private Scan s2;

    private int currPartition;
    private boolean allPartitionsClosed;
    private int keyIterator = 0;

    private Map<Constant, List<Map<String, Constant>>> hashTable;
    private boolean isEmpty;
    Queue<Integer> queue = new LinkedList<>();

    /**
     * Creates a block join scan for the specified LHS scan and
     * RHS scan.
     *
     * @param smallPartitions the smaller partitioned scan
     * @param smallField  the join field of the smaller partitioned table
     * @param largePartitions the larger partitioned scan
     * @param largeField  the join field of the larger partitioned table
     */
    public HashJoinScan(Map<Integer, TempTable> smallPartitions,
                             String smallField, Map<Integer, TempTable> largePartitions,
                             String largeField) {
        this.smallField = smallField;
        this.largeField = largeField;
        this.numPartitions = smallPartitions.size();
        this.smallPartitions = smallPartitions;
        this.largePartitions = largePartitions;
        for (Integer i : smallPartitions.keySet()) {
            queue.offer(i);
        }
        beforeFirst();
    }

    public boolean nextPartition() {
        if (currPartition >= 0) {
            s2.close();
        }

        if (queue.isEmpty()) {
            allPartitionsClosed = true;
            return false;
        }

        currPartition = queue.poll();

        allPartitionsClosed = false;
        s2 = largePartitions.get(currPartition).open();
        s2.beforeFirst();
        if (!s2.next()) {
            isEmpty = !nextPartition();
            return !isEmpty;
        }

        keyIterator = 0;

        Scan s1 = smallPartitions.get(currPartition).open();
        s1.beforeFirst();

        hashTable = new HashMap<>();
        while (s1.next()) {
            if (!hashTable.containsKey(s1.getVal(smallField))) {
                hashTable.put(s1.getVal(smallField), new ArrayList<>());
            }

            Map<String, Constant> row = new HashMap<>();
            for (String field : smallPartitions.get(0).getLayout().schema().fields()) {
                row.put(field, s1.getVal(field));
            }
            hashTable.get(s1.getVal(smallField)).add(row);
        }
        s1.close();

        if (hashTable.isEmpty()) {
            isEmpty = !nextPartition();
            return !isEmpty;
        }

        return true;
    }

    /**
     * Positions the scan before the first record.
     * That is, the LHS scan will be positioned at its
     * first record, and the RHS will be positioned
     * before the first record for the join value.
     *
     * @see Scan#beforeFirst()
     */
    public void beforeFirst() {
        currPartition = -1;
        nextPartition();
    }

    /**
     * Moves the scan to the next record.
     * The method moves to the next RHS record, if possible.
     * Otherwise, it moves to the next LHS record and the
     * first RHS record.
     * If there are no more LHS records, the method returns false.
     *
     * @see Scan#next()
     */

    public boolean next() {
        if (isEmpty) {
            return false;
        }
        while (true) {
            //first check if there are duplicate key values in our hashtable
            if (hashTable.keySet().contains(s2.getVal(largeField)) &&
                keyIterator < hashTable.get(s2.getVal(largeField)).size()) {
                keyIterator++;
                return true;
            }
            while (s2.next()) {
                if (hashTable.keySet().contains(s2.getVal(largeField))) {
                    keyIterator = 1;
                    return true;
                }
            }

            if (!nextPartition()) return false;
        }
    }

    /**
     * Returns the integer value of the specified field.
     *
     * @see Scan#getVal(String)
     */
    public int getInt(String fldname) {
        if (s2.hasField(fldname))
            return s2.getInt(fldname);
        else {
            return hashTable.get(s2.getVal(largeField))
                .get(keyIterator - 1).get(fldname).asInt();
        }
    }

    /**
     * Returns the Constant value of the specified field.
     *
     * @see Scan#getVal(String)
     */
    public Constant getVal(String fldname) {
        if (s2.hasField(fldname))
            return s2.getVal(fldname);
        else {
            return hashTable.get(s2.getVal(largeField)).get(keyIterator - 1).get(fldname);
        }
    }

    /**
     * Returns the string value of the specified field.
     *
     * @see Scan#getVal(String)
     */
    public String getString(String fldname) {
        if (s2.hasField(fldname))
            return s2.getString(fldname);
        else {
            return hashTable.get(s2.getVal(largeField)).
                get(keyIterator - 1).get(fldname).asString();
        }
    }

    /**
     * Returns true if the field is in the schema.
     *
     * @see Scan#hasField(String)
     */
    public boolean hasField(String fldname) {
        return s2.hasField(fldname) ||
            hashTable.get(s2.getVal(largeField)).
                get(keyIterator - 1).keySet().contains(fldname);
    }

    /**
     * Closes the scan by closing its LHS scan and its RHS index.
     *
     * @see Scan#close()
     */
    public void close() {
        if (!allPartitionsClosed)
            s2.close();
    }
}