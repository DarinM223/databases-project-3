package simpledb;

import java.util.*;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    private int ioCostPerPage;
    private int tableid;
    private DbFile dbFile;
    private int numTuples;
    private Map<String, Object> histograms;

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        
        this.ioCostPerPage = ioCostPerPage;
        this.tableid = tableid;
        this.dbFile = Database.getCatalog().getDatabaseFile(tableid);
        this.histograms = new HashMap<String, Object>();

        //create HashMaps to store the mins and maxs
        Map<String, Integer> minMap = new HashMap<String, Integer>();
        Map<String, Integer> maxMap = new HashMap<String, Integer>();

        TransactionId tid = new TransactionId();
        DbFileIterator dbIter = this.dbFile.iterator(tid);

        try {
                dbIter.open();                
                //go through the iterator and store values in the min and max maps
                while (dbIter.hasNext()) {
                        Tuple tuple = dbIter.next();
                        numTuples++;
                        for (int i = 0; i < this.dbFile.getTupleDesc().numFields(); i++) {
                               Field currValue = tuple.getField(i);
                               String fieldName = this.dbFile.getTupleDesc().getFieldName(i);
                               if (currValue.getType().compareTo(Type.INT_TYPE) == 0) {
                                       int val = ((IntField)currValue).getValue();
                                       //if not in minmap, set it to val, otherwise set it to the lower of the current value and val
                                       if (!minMap.containsKey(fieldName)) { 
                                               minMap.put(fieldName, val);
                                       } else if (val < minMap.get(fieldName)) {
                                               minMap.put(fieldName, val);
                                       }

                                       //if not in maxmap, set it to val, otherwise, set it to the larger of the current value and val
                                       if (!maxMap.containsKey(fieldName)) {
                                               maxMap.put(fieldName, val);
                                       } else if (val > maxMap.get(fieldName)) {
                                               maxMap.put(fieldName, val);
                                       }
                               }
                        }
                }
        } catch (DbException e) {
                e.printStackTrace();
        } catch (TransactionAbortedException e) {
                e.printStackTrace();
        } finally {
                dbIter.close();
        }

        dbIter = ((HeapFile)dbFile).iterator(tid);
        try {
                dbIter.open();
                //go through the iterator again and create histogram map
                while (dbIter.hasNext()) {
                        Tuple tuple = dbIter.next();

                        for (int i = 0; i < this.dbFile.getTupleDesc().numFields(); i++) {
                                String fieldName = this.dbFile.getTupleDesc().getFieldName(i);
                                Field currValue = tuple.getField(i);
                                if (currValue.getType().compareTo(Type.INT_TYPE) == 0) {
                                        //if integer type, use the min and max maps to create histogram
                                        if (!histograms.containsKey(fieldName)) {
                                                histograms.put(fieldName, new IntHistogram(NUM_HIST_BINS, minMap.get(fieldName), maxMap.get(fieldName)));
                                        }
                                        ((IntHistogram)histograms.get(fieldName)).addValue(((IntField)currValue).getValue()); 
                                } else {
                                        if (!histograms.containsKey(fieldName)) {
                                                histograms.put(fieldName, new StringHistogram(NUM_HIST_BINS));
                                        }
                                        ((StringHistogram)histograms.get(fieldName)).addValue(((StringField)currValue).getValue());
                                }
                        }
                }
        } catch (DbException e) {
                e.printStackTrace();
        } catch (TransactionAbortedException e) {
                e.printStackTrace();
        } finally {
                dbIter.close();
        }

    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        //return 0;
        return ((HeapFile) dbFile).numPages()*this.ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        //return 0;
        return (int)(numTuples*selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        //return 1.0;
        TupleDesc desc = this.dbFile.getTupleDesc();
        String fieldName = desc.getFieldName(field);

        if (desc.getFieldType(field).compareTo(Type.INT_TYPE) == 0) {
                return ((IntHistogram)histograms.get(fieldName)).avgSelectivity();
        } else {
                return ((StringHistogram)histograms.get(fieldName)).avgSelectivity();
        }
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        //return 1.0;
        TupleDesc desc = this.dbFile.getTupleDesc();
        String fieldName = desc.getFieldName(field);

        if (desc.getFieldType(field).compareTo(Type.INT_TYPE) == 0) {
                return ((IntHistogram)histograms.get(fieldName)).estimateSelectivity(op, ((IntField)constant).getValue());
        } else {
                return ((StringHistogram)histograms.get(fieldName)).estimateSelectivity(op, ((StringField)constant).getValue());
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        //return 0;
        return numTuples;
    }

}
