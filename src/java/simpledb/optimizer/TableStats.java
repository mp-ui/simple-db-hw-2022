package simpledb.optimizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 * <p>
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableStats.class);

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IO_COST_PER_PAGE = 1000;

    public static TableStats getTableStats(String tableName) {
        return statsMap.get(tableName);
    }

    public static void setTableStats(String tableName, TableStats stats) {
        statsMap.put(tableName, stats);
    }

    public static void setStatsMap(Map<String, TableStats> s) {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
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
            int tableId = tableIt.next();
            TableStats s = new TableStats(tableId, IO_COST_PER_PAGE);
            setTableStats(Database.getCatalog().getTableName(tableId), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private final Map<Integer, IntHistogram> intHistogramMap;
    private final Map<Integer, StringHistogram> stringHistogramMap;
    private final int tableId;
    private final int ioCostPerPage;

    private int numTuples;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * @param tableId       The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO. This doesn't differentiate between
     *                      sequential-scan IO and disk seeks.
     */
    public TableStats(int tableId, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        this.tableId = tableId;
        this.ioCostPerPage = ioCostPerPage;
        this.intHistogramMap = new HashMap<>();
        this.stringHistogramMap = new HashMap<>();

        LOGGER.info("start to generate table statistic, tableId={}", tableId);

        TransactionId transactionId = new TransactionId();
        SeqScan seqScan = new SeqScan(transactionId, tableId);
        Map<Integer, Integer> intFieldMinValue = new HashMap<>();
        Map<Integer, Integer> intFieldMaxValue = new HashMap<>();
        Set<Integer> stringFields = new HashSet<>();
        try {
            // 全表扫描，统计整形的最小值和最大值
            LOGGER.info("scan table, tableId={}", tableId);
            seqScan.open();
            TupleDesc tupleDesc = seqScan.getTupleDesc();
            int numFields = tupleDesc.numFields();
            while (seqScan.hasNext()) {
                Tuple next = seqScan.next();
                for (int i = 0; i < numFields; i++) {
                    Field field = next.getField(i);
                    if (field instanceof IntField intField) {
                        intFieldMinValue.put(i, Math.min(intFieldMinValue.getOrDefault(i, Integer.MAX_VALUE),
                                intField.getValue()));
                        intFieldMaxValue.put(i, Math.max(intFieldMaxValue.getOrDefault(i, Integer.MIN_VALUE),
                                intField.getValue()));
                    } else if (field instanceof StringField) {
                        stringFields.add(i);
                    }
                }
                this.numTuples++;
            }

            // 初始化统计图
            for (Integer i : intFieldMinValue.keySet()) {
                this.intHistogramMap.put(i, new IntHistogram(NUM_HIST_BINS,
                        intFieldMinValue.get(i), intFieldMaxValue.get(i)));
            }
            for (Integer i : stringFields) {
                this.stringHistogramMap.put(i, new StringHistogram(NUM_HIST_BINS));
            }

            // 再次扫描表，填充直方图
            LOGGER.info("scan table again, tableId={}", tableId);
            seqScan.rewind();
            while (seqScan.hasNext()) {
                Tuple next = seqScan.next();
                for (int i = 0; i < numFields; i++) {
                    Field field = next.getField(i);
                    if (field instanceof IntField intField) {
                        this.intHistogramMap.get(i).addValue(intField.getValue());
                    } else if (field instanceof StringField stringField) {
                        this.stringHistogramMap.get(i).addValue(stringField.getValue());
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("failed to scan table to generate statistic.", e);
            throw new RuntimeException(e);
        } finally {
            seqScan.close();
        }

        LOGGER.info("generate table statistic succeed, tableId={}", tableId);
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * <p>
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // 扫描成本=总页数*扫描每页的成本
        DbFile dbFile = Database.getCatalog().getDatabaseFile(this.tableId);
        int numPages = dbFile.numPages();
        return numPages * this.ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // 估计返回结果的数量，预计行数=预计总行数*选择性
        return (int) Math.round(this.numTuples * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     *
     * @param field the index of the field
     * @param op    the operator in the predicate
     *              The semantic of the method is that, given the table, and then given a
     *              tuple, of which we do not know the value of the field, return the
     *              expected selectivity. You may estimate this value from the histograms.
     */
    public double avgSelectivity(int field, Predicate.Op op) {
        // TODO: some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * @param field    The field over which the predicate ranges
     * @param op       The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     * predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        if (constant instanceof IntField intField) {
            return this.intHistogramMap.get(field).estimateSelectivity(op, intField.getValue());
        } else if (constant instanceof StringField stringField) {
            return this.stringHistogramMap.get(field).estimateSelectivity(op, stringField.getValue());
        }
        return 1.0;
    }

    /**
     * return the total number of tuples in this table
     */
    public int totalTuples() {
        return this.numTuples;
    }

}
