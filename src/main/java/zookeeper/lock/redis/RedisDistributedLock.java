package zookeeper.lock.redis;

import com.google.common.collect.Maps;
import redis.clients.jedis.JedisPool;
import zookeeper.lock.DistributedReentrantLock;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class RedisDistributedLock implements DistributedReentrantLock {
    private final ConcurrentMap<Thread, LockData> threadData = Maps.newConcurrentMap();
    private RedisLockInternals internals;
    private String lockId;

    RedisDistributedLock(JedisPool jedisPool, String lockId) {
        this.internals = new RedisLockInternals(jedisPool);
        this.lockId = lockId;
    }

    private static class LockData {
        final Thread owningThread;
        final String lockVal;
        final AtomicInteger lockCount = new AtomicInteger(1);

        LockData(Thread owningThread, String lockVal) {
            this.owningThread = owningThread;
            this.lockVal = lockVal;
        }
    }

    @Override
    public boolean tryLock(long timeout, TimeUnit unit) {
        Thread currentThread = Thread.currentThread();
        LockData lockData = threadData.get(currentThread);
        if (lockData != null) {
            lockData.lockCount.incrementAndGet();
            return true;
        }
        String lockVal = internals.tryRedisLock(lockId, timeout, unit);
        if (lockVal != null) {
            LockData newLockData = new LockData(currentThread, lockVal);
            threadData.put(currentThread, newLockData);
            return true;
        }
        return false;
    }

    @Override
    public void unlock() {
        Thread currentThread = Thread.currentThread();
        LockData lockData = threadData.get(currentThread);
        if (lockData == null) {
            throw new IllegalMonitorStateException("You do not own the lock: " + lockId);
        }
        int newLockCount = lockData.lockCount.decrementAndGet();
        if (newLockCount > 0) {
            return;
        }
        if (newLockCount < 0) {
            throw new IllegalMonitorStateException("Lock count has gone negative for lock: " + lockId);
        }
        try {
            internals.unlockRedisLock(lockId, lockData.lockVal);
        } finally {
            threadData.remove(currentThread);
        }
    }
}
