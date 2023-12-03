package simpledb.test;

import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.execution.Filter;
import simpledb.execution.Join;
import simpledb.execution.JoinPredicate;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.HeapFile;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionId;

import java.io.File;

/**
 * @author prince
 * @since 2023/12/3 15:10
 */
public class JoinTest {
    public static void main(String[] argv) {
        // construct a 3-column table schema
        Type[] types = new Type[]{Type.INT_TYPE, Type.STRING_TYPE, Type.INT_TYPE};
        String[] names1 = new String[]{"id", "name", "age"};
        String[] names2 = new String[]{"id", "name", "money"};

        TupleDesc td1 = new TupleDesc(types, names1);
        TupleDesc td2 = new TupleDesc(types, names2);

        // create the tables, associate them with the data files
        // and tell the catalog about the schema  the tables.
        HeapFile table1 = new HeapFile(new File("some_data_file1.dat"), td1);
        Database.getCatalog().addTable(table1, "t1");

        HeapFile table2 = new HeapFile(new File("some_data_file2.dat"), td2);
        Database.getCatalog().addTable(table2, "t2");

        // construct the query: we use two SeqScans, which spoonfeed
        // tuples via iterators into join
        TransactionId tid = new TransactionId();

        SeqScan ss1 = new SeqScan(tid, table1.getId(), "t1");
        SeqScan ss2 = new SeqScan(tid, table2.getId(), "t2");

        // create a filter for the where condition
        Filter sf1 = new Filter(new Predicate(0, Predicate.Op.GREATER_THAN, new IntField(1)), ss1);

        JoinPredicate p = new JoinPredicate(1, Predicate.Op.EQUALS, 1);
        Join j = new Join(p, sf1, ss2);

        try {
            // and run it
            j.open();
            System.out.println(j.getTupleDesc());
            while (j.hasNext()) {
                Tuple tup = j.next();
                System.out.println(tup);
            }
            j.close();
            Database.getBufferPool().transactionComplete(tid);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
