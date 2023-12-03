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

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {
    private static final Logger LOGGER = LoggerFactory.getLogger(Delete.class);

    @Serial
    private static final long serialVersionUID = 2977972771417424899L;

    private final TupleDesc tupleDesc;
    private final TransactionId transactionId;
    private final OpIterator child;
    private boolean called;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param transactionId The transaction this deletes runs in
     * @param child         The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId transactionId, OpIterator child) {
        this.transactionId = transactionId;
        this.child = child;
        this.called = false;

        // 初始化TupleDesc，该操作返回一行一列，一个整数代表影响的行数
        Type[] types = new Type[]{Type.INT_TYPE};
        String[] names = new String[]{"deleted count"};
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (this.called) {
            return null;
        }

        this.called = true;
        int deleteCount = 0;
        while (child.hasNext()) {
            Tuple next = child.next();
            try {
                Database.getBufferPool().deleteTuple(transactionId, next);
                deleteCount++;
            } catch (IOException e) {
                LOGGER.error("delete tuple " + next + " error. ", e);
            }
        }
        // 返回删除的行数
        Tuple tuple = new Tuple(this.getTupleDesc());
        tuple.setField(0, new IntField(deleteCount));
        return tuple;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};
    }
}
