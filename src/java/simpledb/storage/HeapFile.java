package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc tupleDesc;

    private final ReentrantReadWriteLock readWriteLock;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.tupleDesc = td;
        this.readWriteLock = new ReentrantReadWriteLock(true);
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        readWriteLock.readLock().lock();
        try {
            int numPages = this.numPages();
            int pageNumber = pid.getPageNumber();
            if (!Objects.equals(pid.getTableId(), this.getId())) {
                throw new IllegalArgumentException("the page does not exist in this file.");
            }

            // 页编号超出总页数，则返回空白页面（新增页面的场景使用）
            if (pageNumber >= numPages) {
                HeapPageId pageId = new HeapPageId(this.getId(), pageNumber);
                byte[] data = HeapPage.createEmptyPageData() ;
                return new HeapPage(pageId, data);
            }

            try (RandomAccessFile randomAccessFile = new RandomAccessFile(this.file, "r")) {
                int pageSize = BufferPool.getPageSize();
                byte[] bytes = new byte[pageSize];
                randomAccessFile.seek(((long) pageNumber) * pageSize);
                randomAccessFile.read(bytes);
                return new HeapPage((HeapPageId) pid, bytes);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        readWriteLock.writeLock().lock();
        try {
            PageId pageId = page.getId();
            int pageNumber = pageId.getPageNumber();
            try (RandomAccessFile randomAccessFile = new RandomAccessFile(this.file, "rw")) {
                int pageSize = BufferPool.getPageSize();
                // 找到该页面存放的位置
                randomAccessFile.seek((long) pageNumber * pageSize);
                randomAccessFile.write(page.getPageData());
            }
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    @Override
    public int numPages() {
        long length = this.file.length();
        int pageSize = BufferPool.getPageSize();
        return (int) Math.ceil((double) length / pageSize);
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // 从头到尾，找到第一个有空位的页面
        boolean inserted = false;
        HeapPage page = null;
        for (int i = 0; i <= this.numPages(); i++) {
            PageId pageId = new HeapPageId(this.getId(), i);
            page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
            if (page.getNumUnusedSlots() > 0) {
                page.insertTuple(t);
                inserted = true;
                break;
            }
        }
        if (!inserted) {
            throw new DbException("fail to insert tuple.");
        }
        return Collections.singletonList(page);
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        RecordId recordId = t.getRecordId();
        PageId pageId = recordId.getPageId();
        int tableId = pageId.getTableId();
        if (!Objects.equals(tableId, this.getId())) {
            throw new DbException("the tuple is not a member of the file.");
        }

        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        page.deleteTuple(t);
        return Collections.singletonList(page);
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid, this);
    }

}

