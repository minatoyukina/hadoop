package zookeeper.lock;

public interface DistributedLockTemplate {
    Object execute(String lockId, int timeout, Callback callback);
}
