package simpledb.execution;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.io.Serial;
import java.util.Objects;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {
    private static final Logger LOGGER = LoggerFactory.getLogger(Insert.class);

    @Serial
    private static final long serialVersionUID = -7029138916793282005L;

    private final TupleDesc tupleDesc;
    private final TransactionId transactionId;
    private final int tableId;
    private final OpIterator child;
    private boolean called;

    /**
     * Constructor.
     *
     * @param transactionId The transaction running the insert.
     * @param child         The child operator from which to read tuples to be inserted.
     * @param tableId       The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to insert.
     */
    public Insert(TransactionId transactionId, OpIterator child, int tableId) throws DbException {
        // 校验插入的数据与表结构是否匹配
        if (!Objects.equals(child.getTupleDesc(), Database.getCatalog().getTupleDesc(tableId))) {
            throw new DbException("TupleDesc of child differs from table into which we are to insert.");
        }

        this.transactionId = transactionId;
        this.child = child;
        this.tableId = tableId;
        this.called = false;

        // 初始化TupleDesc，该操作返回一行一列，一个整数代表影响的行数
        Type[] types = new Type[]{Type.INT_TYPE};
        String[] names = new String[]{"inserted count"};
        this.tupleDesc = new TupleDesc(types, names);
    }

    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        this.child.open();
        this.called = false;
    }

    public void close() {
        super.close();
        this.child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.child.rewind();
        this.called = false;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (this.called) {
            return null;
        }

        this.called = true;
        int insertedCount = 0;
        while (child.hasNext()) {
            Tuple next = child.next();
            try {
                Database.getBufferPool().insertTuple(transactionId, tableId, next);
                insertedCount++;
            } catch (IOException e) {
                LOGGER.error("insert tuple " + next + " error. ", e);
            }
        }
        // 返回插入的行数
        Tuple tuple = new Tuple(this.getTupleDesc());
        tuple.setField(0, new IntField(insertedCount));
        return tuple;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};
    }
}
