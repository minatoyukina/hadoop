package zookeeper.lock.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zookeeper.lock.DistributedReentrantLock;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ZKReentrantLock implements DistributedReentrantLock {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZKReentrantLock.class);
    private static final ScheduledExecutorService service = Executors.newScheduledThreadPool(10);

    public static final String ROOT_PATH = "/ROOT_LOCK";

    private long delayTimeForClean = 1000;
    private InterProcessMutex interProcessMutex = null;
    private String path;
    private CuratorFramework client;

    public ZKReentrantLock(CuratorFramework client, String lockId) {
        init(client, lockId);
    }

    private void init(CuratorFramework client, String lockId) {
        this.client = client;
        this.path = ROOT_PATH + lockId;
        interProcessMutex = new InterProcessMutex(client, this.path);
    }

    @Override
    public boolean tryLock(long timeout, TimeUnit unit) throws InterruptedException {
        try {
            return interProcessMutex.acquire(timeout, unit);
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public void unlock() {
        try {
            interProcessMutex.release();
        } catch (Throwable e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            service.schedule(new Cleaner(client, path), delayTimeForClean, TimeUnit.SECONDS);
        }
    }

    private static class Cleaner implements Runnable {
        private CuratorFramework client;
        String path;

        Cleaner(CuratorFramework client, String path) {
            this.client = client;
            this.path = path;
        }

        @Override
        public void run() {
            try {
                List<String> list = client.getChildren().forPath(path);
                if (list == null || list.isEmpty()) {
                    client.delete().forPath(path);
                }
            } catch (KeeperException.NoNodeException | KeeperException.NotEmptyException e1) {

            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }
}
