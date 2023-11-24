package simpledb.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    private static final Logger LOGGER = LoggerFactory.getLogger(BufferPool.class);

    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;
    /**
     * 冷热链表中冷数据的比例
     */
    private static final double DEFAULT_OLD_DATA_RATE = 0.2;
    /**
     * 冷数据在冷链表中的保存时间（毫秒），默认1秒
     */
    private static final Integer DEFAULT_OLD_BLOCK_TIMES = 1000;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    private final int numYoungSize;
    private final int numOldSize;

    private final Map<PageId, Date> lastUsedTimeMap = new HashMap<>();
    private final LinkedHashMap<PageId, Page> youngMap = new LinkedHashMap<>();
    private final LinkedHashMap<PageId, Page> oldMap = new LinkedHashMap<>();

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numOldSize = (int) (numPages * DEFAULT_OLD_DATA_RATE);
        this.numYoungSize = numPages - numOldSize;
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // 简单LRU算法实现
        Date now = new Date();
        DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        LOGGER.info("Prepare to get page from youngMap. tid={}, tableId={}, pageNo={}, perm={}",
                tid.getId(), pid.getTableId(), pid.getPageNumber(), perm);
        Page page = youngMap.get(pid);
        if (Objects.isNull(page)) {
            LOGGER.info("Page not found in young map, attempt to get it in old map. tid={}, tableId={}, pageNo={}, " +
                    "perm={}", tid.getId(), pid.getTableId(), pid.getPageNumber(), perm);
            page = oldMap.get(pid);
            if (Objects.isNull(page)) {
                LOGGER.info("Page not found in old map, attempt to read it from disk. tid={}, tableId={}, " +
                        "pageNo={}, perm={}", tid.getId(), pid.getTableId(), pid.getPageNumber(), perm);
                page = dbFile.readPage(pid);
                LOGGER.info("Read page from disk. tid={}, tableId={}, pageNo={}, perm={}",
                        tid.getId(), pid.getTableId(), pid.getPageNumber(), perm);
                // 尽量使用缓存
                if (oldMap.size() < numOldSize) {
                    this.putIntoMap(oldMap, page, numYoungSize);
                } else if (youngMap.size() < numYoungSize) {
                    // young区有位置，但old区满，则将old区的一个页面移动到young区，然后将新页面放入old区
                    if (!oldMap.isEmpty()) {
                        PageId tmpPageId = this.oldMap.keySet().stream().findFirst().get();
                        Page tmpPage = this.oldMap.get(tmpPageId);
                        this.removeFromMap(oldMap, tmpPageId);
                        this.putIntoMap(youngMap, tmpPage, numYoungSize);
                        this.putIntoMap(oldMap, page, numOldSize);
                    } else {
                        this.putIntoMap(youngMap, page, numYoungSize);
                    }
                } else {
                    this.putIntoMap(youngMap, page, numYoungSize);
                }
            } else {
                LOGGER.info("Got page from oldMap. tid={}, tableId={}, pageNo={}, perm={}",
                        tid.getId(), pid.getTableId(), pid.getPageNumber(), perm);
                this.removeFromMap(oldMap, pid);
                Date lastUsedTime = lastUsedTimeMap.getOrDefault(pid, now);
                if (now.getTime() - lastUsedTime.getTime() > DEFAULT_OLD_BLOCK_TIMES) {
                    this.putIntoMap(youngMap, page, numYoungSize);
                } else {
                    this.putIntoMap(oldMap, page, numOldSize);
                }
            }
        } else {
            LOGGER.info("Got page from youngMap. tid={}, tableId={}, pageNo={}, perm={}",
                    tid.getId(), pid.getTableId(), pid.getPageNumber(), perm);
            this.removeFromMap(youngMap, pid);
            this.putIntoMap(youngMap, page, numYoungSize);
        }
        // 更新最后使用时间
        lastUsedTimeMap.put(pid, now);
        return page;
    }

    /**
     * 从map中移除page
     */
    private void removeFromMap(LinkedHashMap<PageId, Page> map, PageId pageId) {
        map.remove(pageId);
    }

    /**
     * 将page放入map中，如果超过最大长度，需要移除最久未被使用的page
     */
    private void putIntoMap(LinkedHashMap<PageId, Page> map, Page page, int limit) {
        map.put(page.getId(), page);
        if (map.size() > limit) {
            map.remove(map.keySet().iterator().next());
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // TODO: some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // TODO: some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // TODO: some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // TODO: some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // TODO: some code goes here
        // not necessary for lab1
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // TODO: some code goes here
        // not necessary for lab1
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // TODO: some code goes here
        // not necessary for lab1

    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void removePage(PageId pid) {
        // TODO: some code goes here
        // not necessary for lab1
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // TODO: some code goes here
        // not necessary for lab1
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // TODO: some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // TODO: some code goes here
        // not necessary for lab1
    }

}
