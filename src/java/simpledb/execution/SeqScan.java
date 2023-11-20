package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.DbFile;
import simpledb.storage.DbFileIterator;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.Serial;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {
    @Serial
    private static final long serialVersionUID = 6090603541971828516L;

    private final TransactionId tid;

    private int tableId;
    private String tableAlias;
    private DbFileIterator dbFileIterator;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid        The transaction this scan is running as a part of.
     * @param tableId    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableId, String tableAlias) {
        this.tid = tid;
        this.tableId = tableId;
        this.tableAlias = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    /**
     * @return return the table name of the table the operator scans. This should
     * be the actual name of the table in the catalog of the database
     */
    public String getTableName() {
        return Database.getCatalog().getTableName(this.tableId);
    }

    /**
     * @return Return the alias of the table this operator scans.
     */
    public String getAlias() {
        return this.tableAlias;
    }

    /**
     * Reset the tableId, and tableAlias of this operator.
     *
     * @param tableId    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public void reset(int tableId, String tableAlias) {
        this.tableId = tableId;
        this.tableAlias = tableAlias;
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        TupleDesc tupleDesc = Database.getCatalog().getTupleDesc(this.tableId);
        int numFields = tupleDesc.numFields();
        Type[] types = new Type[numFields];
        String[] names = new String[numFields];
        for (int i = 0; i < numFields; i++) {
            types[i] = tupleDesc.getFieldType(i);
            names[i] = this.tableAlias + "." + tupleDesc.getFieldName(i);
        }
        return new TupleDesc(types, names);
    }

    public void open() throws DbException, TransactionAbortedException {
        this.dbFileIterator = Database.getCatalog().getDatabaseFile(this.tableId).iterator(this.tid);
        this.dbFileIterator.open();
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        if (Objects.isNull(this.dbFileIterator)) {
            throw new IllegalStateException("iterator not open");
        }
        return this.dbFileIterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        if (!this.hasNext()) {
            throw new NoSuchElementException();
        }
        return this.dbFileIterator.next();
    }

    public void close() {
        this.dbFileIterator = null;
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        if (Objects.isNull(this.dbFileIterator)) {
            throw new IllegalStateException("iterator not open");
        }
        this.dbFileIterator.rewind();
    }
}
