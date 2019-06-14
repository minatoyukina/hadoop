package zookeeper.lock.zk;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zookeeper.lock.Callback;
import zookeeper.lock.DistributedLockTemplate;

import java.util.concurrent.TimeUnit;

public class ZKDistributedLockTemplate implements DistributedLockTemplate {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZKDistributedLockTemplate.class);
    private CuratorFramework client;

    public ZKDistributedLockTemplate(CuratorFramework client) {
        this.client = client;
    }

    private boolean tryLock(ZKReentrantLock lock, Long timeout) throws Exception {
        return lock.tryLock(timeout, TimeUnit.MILLISECONDS);
    }

    @Override
    public Object execute(String lockId, int timeout, Callback callback) {
        ZKReentrantLock reentrantLock = null;
        boolean getLock = false;
        try {
            reentrantLock = new ZKReentrantLock(client, lockId);
            if (tryLock(reentrantLock, (long) timeout)) {
                getLock = true;
                return callback.onGetLock();
            } else {
                return callback.onTimeout();
            }
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage(), e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            if (getLock) {
                reentrantLock.unlock();
            }
        }
        return null;
    }
}
