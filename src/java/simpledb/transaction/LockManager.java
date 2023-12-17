package simpledb.transaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.storage.PageId;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author prince
 * @since 2023/12/16 14:29
 */
public class LockManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(LockManager.class);
    private static final long LOCK_RETRY_INTERVAL = 100L;

    private final Map<PageId, Set<Lock>> lockMap = new ConcurrentHashMap<>();

    public void acquire(TransactionId transactionId, PageId pageId, Permissions permissions) throws DbException {
        if (Objects.equals(permissions, Permissions.READ_ONLY)) {
            acquire(transactionId, pageId, LockType.READ_LOCK);
        } else {
            acquire(transactionId, pageId, LockType.WRITE_LOCK);
        }
    }

    public void acquire(TransactionId transactionId, PageId pageId, LockType lockType) throws DbException {
        LOGGER.info("Prepare to acquire a lock. tid={}, pageId={}, type={}", transactionId, pageId, lockType);

        // 获取页面上的锁列表
        lockMap.putIfAbsent(pageId, new HashSet<>());
        Set<Lock> locks = lockMap.get(pageId);

        Lock lock = new Lock(transactionId, pageId, lockType);
        // 持有同类型的锁
        if (locks.contains(lock)) {
            LOGGER.info("Already has lock. tid={}, pageId={}, type={}", transactionId, pageId, lockType);
            return;
        }
        // 持有写锁拿读锁
        if (lockType == LockType.READ_LOCK && locks.contains(new Lock(transactionId, pageId, LockType.WRITE_LOCK))) {
            LOGGER.info("Already has write lock but acquire a read lock. tid={}, pageId={}, type={}",
                    transactionId, pageId, lockType);
            return;
        }
        // 尝试获取锁
        int retryTime = 0;
        while (true) {
            synchronized (locks) {
                Lock firstLock;
                // 具备直接上锁条件
                if (locks.isEmpty() || ((firstLock = locks.iterator().next()).getLockType() == LockType.READ_LOCK &&
                        lockType == LockType.READ_LOCK)) {
                    LOGGER.info("Succeed to acquire a lock. tid={}, pageId={}, lockType={}",
                            transactionId, pageId, lockType);
                    locks.add(lock);
                    break;
                }
                // 具备锁升级条件
                if (locks.size() == 1 && firstLock.getLockType() == LockType.READ_LOCK &&
                        firstLock.getTransactionId().equals(transactionId) && lockType == LockType.WRITE_LOCK) {
                    LOGGER.info("Upgrade lock. tid={}, pageId={}, lockType={}", transactionId, pageId, lockType);
                    locks.add(lock);
                    locks.remove(firstLock);
                    break;
                }
            }
            LOGGER.info("Fail to acquire a lock, retry now. tid={}, pageId={}, lockType={}, retryTime={}",
                    transactionId, pageId, lockType, ++retryTime);
            try {
                Thread.sleep(LOCK_RETRY_INTERVAL);
            } catch (InterruptedException e) {
                throw new DbException("fail to acquire a lock, message = " + e.getMessage());
            }
        }
    }

    public void release(TransactionId transactionId, PageId pageId) {
        Set<Lock> locks = lockMap.get(pageId);
        if (locks == null) {
            return;
        }
        synchronized (locks) {
            locks.removeIf(next -> Objects.equals(next.getTransactionId(), transactionId));
            if (locks.isEmpty()) {
                lockMap.remove(pageId);
            }
        }
    }

    public boolean holdsLock(TransactionId transactionId, PageId pageId) {
        Set<Lock> locks = lockMap.get(pageId);
        if (locks == null || locks.isEmpty()) {
            return false;
        }
        synchronized (locks) {
            return locks.stream().anyMatch(item -> Objects.equals(item.getTransactionId(), transactionId));
        }
    }

    public boolean holdsLock(PageId pageId) {
        Set<Lock> locks = lockMap.get(pageId);
        return locks != null && !locks.isEmpty();
    }
}
