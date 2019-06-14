package zookeeper.lock;

import java.util.concurrent.TimeUnit;

public interface DistributedReentrantLock {
    boolean tryLock(long timeout, TimeUnit unit) throws InterruptedException;

    void unlock();
}
