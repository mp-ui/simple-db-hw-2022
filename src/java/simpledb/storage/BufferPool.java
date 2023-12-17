package simpledb.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.LockManager;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
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

    private final LockManager lockManager = new LockManager();

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
        lockManager.acquire(tid, pid, perm);
        try {
            // 简单LRU算法实现
            Date now = new Date();
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            LOGGER.info("Prepare to get page from youngMap. tid={}, pageId={}, perm={}", tid, pid, perm);
            Page page = youngMap.get(pid);
            if (Objects.isNull(page)) {
                LOGGER.info("Page not found in young map, attempt to get it in old map. tid={}, pageId={}, perm={}",
                        tid, pid, perm);
                page = oldMap.get(pid);
                if (Objects.isNull(page)) {
                    LOGGER.info("Page not found in old map, attempt to read it from disk. tid={}, pageId={}, perm={}",
                            tid, pid, perm);
                    page = dbFile.readPage(pid);
                    LOGGER.info("Read page from disk. tid={}, pageId={}, perm={}", tid, pid, perm);
                    // 存入BufferPool
                    this.addPage(page);
                } else {
                    LOGGER.info("Got page from oldMap. tid={}, pageId={}, perm={}", tid, pid, perm);
                    this.removeFromMap(oldMap, pid);
                    Date lastUsedTime = lastUsedTimeMap.getOrDefault(pid, now);
                    if (now.getTime() - lastUsedTime.getTime() > DEFAULT_OLD_BLOCK_TIMES &&
                            youngMap.size() < numYoungSize) {
                        this.putIntoMap(youngMap, page, numYoungSize);
                    } else {
                        this.putIntoMap(oldMap, page, numOldSize);
                    }
                }
            } else {
                LOGGER.info("Got page from youngMap. tid={}, pageId={}, perm={}", tid, pid, perm);
                this.removeFromMap(youngMap, pid);
                this.putIntoMap(youngMap, page, numYoungSize);
            }
            // 更新最后使用时间
            lastUsedTimeMap.put(pid, now);
            return page;
        } catch (Exception e) {
            LOGGER.error("Release lock because of exception. tid={}, pageId={}, perm={}", tid, pid, perm);
            lockManager.release(tid, pid);
            throw e;
        }
    }

    /**
     * 从map中移除page
     */
    private synchronized void removeFromMap(LinkedHashMap<PageId, Page> map, PageId pageId) {
        map.remove(pageId);
    }

    /**
     * 将page放入map中，如果超过最大长度，需要移除最久未被使用的page
     */
    private synchronized void putIntoMap(LinkedHashMap<PageId, Page> map, Page page, int limit) {
        map.put(page.getId(), page);
        if (map.size() > limit) {
            map.remove(map.keySet().iterator().next());
            lastUsedTimeMap.remove(page.getId());
        }
    }

    private synchronized void addPage(Page page) throws DbException {
        // 校验该页面是否已在BufferPool中存在
        PageId pageId = page.getId();
        if (youngMap.containsKey(pageId) || oldMap.containsKey(pageId)) {
            return;
        }
        // 尽量使用缓存
        if (oldMap.size() < numOldSize) {
            this.putIntoMap(oldMap, page, numYoungSize);
        } else if (youngMap.size() < numYoungSize) {
            // young区有位置，但old区满，则将old区的一个页面移动到young区，然后将新页面放入old区
            if (!oldMap.isEmpty()) {
                PageId tmpPageId = this.oldMap.keySet().stream().findFirst().get();
                Page tmpPage = this.oldMap.get(tmpPageId);
                this.removeFromMap(this.oldMap, tmpPageId);
                this.putIntoMap(youngMap, tmpPage, numYoungSize);
                this.putIntoMap(oldMap, page, numOldSize);
            } else {
                this.putIntoMap(youngMap, page, numYoungSize);
            }
        } else {
            // 均满，则驱逐一个页面
            this.evictPage();
            // 重新添加
            this.addPage(page);
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     * 当BufferPool满时，用于驱逐一个页面
     */
    private synchronized void evictPage() throws DbException {
        LOGGER.info("Prepare to evict a page from BufferPool.");
        List<Map.Entry<PageId, Page>> entries = new ArrayList<>();
        entries.addAll(oldMap.entrySet());
        entries.addAll(youngMap.entrySet());
        // 找到最久未使用的一个脏页面
        for (Map.Entry<PageId, Page> entry : entries) {
            PageId pageId = entry.getKey();
            Page page = entry.getValue();
            if (Objects.isNull(page.isDirty())) {
                LOGGER.info("Evict page {} from BufferPool.", pageId);
                // 如果该页面被其他事务持有锁，则不能将其刷到磁盘，因为事务有可能会将其修改，修改过的页面必须要事务提交后才能刷新到磁盘
                if (!lockManager.holdsLock(pageId)) {
                    this.flushPage(pageId);
                }
                this.removePage(pageId);
                return;
            }
        }
        // 如果仍然找不到则抛出异常
        throw new DbException("All pages are dirty, can not evict any page in BufferPool.");
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
        LOGGER.info("Unsafe release page. tid={}, pid={}", tid, pid);
        lockManager.release(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        this.transactionComplete(tid, true);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public synchronized void transactionComplete(TransactionId tid, boolean commit) {
        if (commit) {
            LOGGER.info("commit transaction. tid={}", tid);
        } else {
            LOGGER.info("abort transaction. tid={}", tid);
        }
        List<Map.Entry<PageId, Page>> entries = new ArrayList<>();
        entries.addAll(oldMap.entrySet());
        entries.addAll(youngMap.entrySet());
        for (Map.Entry<PageId, Page> entry : entries) {
            PageId pageId = entry.getKey();
            Page page = entry.getValue();
            if (!lockManager.holdsLock(tid, pageId)) {
                continue;
            }
            if (commit) {
                // 如果提交事务，则将页面刷新到磁盘后解锁
                flushPage(pageId);
            } else if (Objects.equals(page.isDirty(), tid)) {
                // 如果回滚事务，并且该页面是脏页（脏页一定是这个事务修改的，因为写锁只能被一个事务持有），则丢弃该页面
                this.removePage(pageId);
            }
            lockManager.release(tid, pageId);
        }
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
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pages = dbFile.insertTuple(tid, t);
        for (Page page : pages) {
            page.markDirty(true, tid);
            this.addPage(page);
        }
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
        RecordId recordId = t.getRecordId();
        DbFile dbFile = Database.getCatalog().getDatabaseFile(recordId.getPageId().getTableId());
        List<Page> pages = dbFile.deleteTuple(tid, t);
        for (Page page : pages) {
            page.markDirty(true, tid);
            this.addPage(page);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (Page page : oldMap.values()) {
            if (Objects.nonNull(page.isDirty())) {
                this.flushPage(page.getId());
            }
        }
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
        this.youngMap.remove(pid);
        this.oldMap.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) {
        Page page = oldMap.get(pid);
        if (Objects.isNull(page)) {
            youngMap.get(pid);
        }
        if (Objects.nonNull(page) && Objects.nonNull(page.isDirty())) {
            DbFile dbFile = Database.getCatalog().getDatabaseFile(page.getId().getTableId());
            try {
                LOGGER.info("Flush page {} to disk.", pid);
                dbFile.writePage(page);
            } catch (IOException e) {
                LOGGER.error("An error occurred while flush a page to disk.", e);
                throw new RuntimeException(e);
            }
            page.markDirty(false, null);
        }
    }

}
