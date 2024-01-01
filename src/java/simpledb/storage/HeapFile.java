package simpledb.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    private static final Logger LOGGER = LoggerFactory.getLogger(HeapFile.class);

    private final File file;
    private final TupleDesc tupleDesc;

    private final ReentrantReadWriteLock readWriteLock;
    private int numPage;

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
        // 计算页面数量
        long length = this.file.length();
        int pageSize = BufferPool.getPageSize();
        this.numPage = (int) Math.ceil((double) length / pageSize);
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
                // 超出页面总大小新增空白页面时，需要更新页面的数量
                this.numPage = Math.max(this.numPage, pageNumber + 1);
                HeapPageId pageId = new HeapPageId(this.getId(), pageNumber);
                byte[] data = HeapPage.createEmptyPageData();
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
        return this.numPage;
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        LOGGER.info("Prepare to insert tuple. tid={}", tid);
        // 从头到尾，找到第一个有空位的页面
        boolean inserted = false;
        HeapPage page = null;
        for (int i = 0; i <= this.numPages(); i++) {
            PageId pageId = new HeapPageId(this.getId(), i);
            // 先按照READ_ONLY权限获取页面，确认该页面有空闲插槽时再以READ_WRITE权限申请页面
            page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
            if (page.getNumUnusedSlots() > 0) {
                LOGGER.info("Found a page to insert tuple. tid={}, pid={}", tid, pageId);
                // 升级成写锁
                Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
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

