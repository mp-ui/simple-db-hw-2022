package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.Iterator;
import java.util.Objects;

/**
 * @author prince
 * @date 2023/11/19 00:51
 */
public class HeapFileIterator extends AbstractDbFileIterator {

    private final TransactionId tid;
    private final HeapFile heapFile;
    private final int numPages;

    private int currentPageNumber = 0;
    private HeapPage currentPage;
    private Iterator<Tuple> iterator;

    public HeapFileIterator(TransactionId tid, HeapFile heapFile) {
        this.tid = tid;
        this.heapFile = heapFile;
        this.numPages = heapFile.numPages();
    }

    @Override
    protected Tuple readNext() throws DbException, TransactionAbortedException {
        // The iterator is not open.
        if (Objects.isNull(this.iterator)) {
            return null;
        }
        if (this.iterator.hasNext()) {
            return this.iterator.next();
        }
        while (this.currentPageNumber < this.numPages) {
            this.currentPageNumber++;
            this.currentPage = getPage(this.currentPageNumber);
            if (Objects.isNull(this.currentPage)) {
                break;
            }
            this.iterator = this.currentPage.iterator();
            if (this.iterator.hasNext()) {
                return this.iterator.next();
            }
        }
        return null;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        this.currentPageNumber = 0;
        this.currentPage = getPage(this.currentPageNumber);
        if (Objects.isNull(this.currentPage)) {
            return;
        }
        this.iterator = this.currentPage.iterator();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        this.open();
    }

    @Override
    public void close() {
        super.close();
        this.currentPageNumber = 0;
        this.currentPage = null;
        this.iterator = null;
    }

    private HeapPage getPage(int pageNumber) throws TransactionAbortedException, DbException {
        if (pageNumber < 0 || pageNumber >= this.numPages) {
            return null;
        }
        HeapPageId pageId = new HeapPageId(this.heapFile.getId(), pageNumber);
        return (HeapPage) Database.getBufferPool().getPage(this.tid, pageId, Permissions.READ_ONLY);
    }
}
