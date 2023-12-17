package simpledb.transaction;

import simpledb.storage.PageId;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * @author prince
 * @since 2023/12/17 00:59
 */
public class Lock implements Serializable {
    @Serial
    private static final long serialVersionUID = -7222407509904109601L;

    private final TransactionId transactionId;
    private final PageId pageId;
    private final LockType lockType;

    public Lock(TransactionId transactionId, PageId pageId, LockType lockType) {
        this.transactionId = transactionId;
        this.pageId = pageId;
        this.lockType = lockType;
    }

    public TransactionId getTransactionId() {
        return transactionId;
    }

    public PageId getPageId() {
        return pageId;
    }

    public LockType getLockType() {
        return lockType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Lock lock = (Lock) o;
        return Objects.equals(transactionId, lock.transactionId) &&
                Objects.equals(pageId, lock.pageId) && Objects.equals(lockType, lock.lockType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transactionId, pageId, lockType);
    }
}
