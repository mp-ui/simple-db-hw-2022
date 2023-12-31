package simpledb.transaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.storage.PageId;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author prince
 * @since 2023/12/16 14:29
 */
public class LockManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(LockManager.class);
    private static final long LOCK_RETRY_INTERVAL_MIN = 200L;
    private static final long LOCK_RETRY_INTERVAL_MAX = 500L;
    // 当重试次数超过这个数时，视为超时，抛出异常
    private static final int DEADLOCK_SUSPECT__RETRY_TIME_THRESHOLD = 5;
    // 当重试次数达到这个数时，检测死锁
    private static final int DEADLOCK_DETECT_RETRY_TIME_THRESHOLD = 1;
    private static final Random random = new Random();

    private final Map<PageId, Set<Lock>> lockMap = new ConcurrentHashMap<>();
    private final Map<TransactionId, Set<TransactionId>> waitingChart = new ConcurrentHashMap<>();

    public void acquire(TransactionId transactionId, PageId pageId, Permissions permissions)
            throws DbException, TransactionAbortedException {
        if (Objects.equals(permissions, Permissions.READ_ONLY)) {
            acquire(transactionId, pageId, LockType.READ_LOCK);
        } else {
            acquire(transactionId, pageId, LockType.WRITE_LOCK);
        }
    }

    public void acquire(TransactionId transactionId, PageId pageId, LockType lockType)
            throws DbException, TransactionAbortedException {
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
                if (retryTime == 0) {
                    // 在等待图中新增节点
                    waitingChart.put(transactionId, locks.stream()
                            .map(Lock::getTransactionId)
                            .filter(item -> !Objects.equals(item, transactionId))
                            .collect(Collectors.toUnmodifiableSet()));
                }
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
            if (retryTime == DEADLOCK_SUSPECT__RETRY_TIME_THRESHOLD) {
                LOGGER.error("Deadlock detected because of timeout, abort transaction.");
                throw new TransactionAbortedException();
            }
            if (retryTime == DEADLOCK_DETECT_RETRY_TIME_THRESHOLD) {
                detectDeadLock();
            }
            LOGGER.info("Fail to acquire a lock, retry now. tid={}, pageId={}, lockType={}, retryTime={}",
                    transactionId, pageId, lockType, ++retryTime);
            try {
                long sleepTime = random.nextLong(LOCK_RETRY_INTERVAL_MAX + 1 - LOCK_RETRY_INTERVAL_MIN) +
                        LOCK_RETRY_INTERVAL_MIN;
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                throw new DbException("fail to acquire a lock, message = " + e.getMessage());
            }
        }
        // 成功获取锁之后，在等待图中删除节点
        waitingChart.remove(transactionId);
    }

    private void detectDeadLock() throws TransactionAbortedException {
        // 保存waitingChart的拷贝，保证线程安全
        Map<TransactionId, Set<TransactionId>> waitingChartCopy = new HashMap<>();
        // 计算每个节点的入度
        Map<TransactionId, Integer> inDegree = new HashMap<>();
        waitingChart.forEach((transactionId, toTransactionIds) -> {
            waitingChartCopy.put(transactionId, toTransactionIds);
            inDegree.putIfAbsent(transactionId, 0);
            for (TransactionId toTransactionId : toTransactionIds) {
                inDegree.put(toTransactionId, inDegree.getOrDefault(toTransactionId, 0) + 1);
            }
        });
        // 保存到优先队列
        PriorityQueue<NodeInDegree> priorityQueue = new PriorityQueue<>();
        inDegree.forEach((transactionId, in) -> priorityQueue.add(new NodeInDegree(transactionId, in)));
        // 循环遍历
        while (!priorityQueue.isEmpty()) {
            NodeInDegree nodeInDegree = priorityQueue.poll();
            if (!Objects.equals(inDegree.get(nodeInDegree.node()), nodeInDegree.inDegree())) {
                // 旧数据，直接跳过
                continue;
            }
            if (nodeInDegree.inDegree() > 0) {
                LOGGER.error("Deadlock detected because of waiting chart circle, abort transaction.");
                throw new TransactionAbortedException();
            }
            // 逻辑移除该节点，该节点指向的节点的入度-1
            Set<TransactionId> toTransactionIds = waitingChartCopy.get(nodeInDegree.node());
            if (Objects.isNull(toTransactionIds)) {
                continue;
            }
            for (TransactionId toTransactionId : toTransactionIds) {
                Integer in = inDegree.get(toTransactionId);
                in -= 1;
                inDegree.put(toTransactionId, in);
                priorityQueue.add(new NodeInDegree(toTransactionId, in));
            }
        }
    }

    public void release(TransactionId transactionId, PageId pageId) {
        LOGGER.info("Release lock. tid={}, pid={}", transactionId, pageId);
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

    private record NodeInDegree(TransactionId node, int inDegree) implements Comparable<NodeInDegree> {
        @Override
        public int compareTo(NodeInDegree o) {
            if (this.inDegree < o.inDegree) {
                return -1;
            } else if (this.inDegree > o.inDegree) {
                return 1;
            }
            return 0;
        }
    }
}
