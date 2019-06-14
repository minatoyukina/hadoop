package zookeeper.lock.zk;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

public class ZKReentrantLockCleanerTask extends TimerTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZKReentrantLockCleanerTask.class);
    private CuratorFramework client;
    private Timer timer;

    private long period = 5000;
    private int maxRetries = 3;
    private final int baseSleepTimeMs = 1000;

    public ZKReentrantLockCleanerTask(String zookeeperAddress) {
        try {
            RetryPolicy retryPolicy = new ExponentialBackoffRetry(baseSleepTimeMs, maxRetries);
            client = CuratorFrameworkFactory.newClient(zookeeperAddress, retryPolicy);
            client.start();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    public void start() {
        timer.schedule(this, 0, period);
    }

    private boolean isEmpty(List<String> list) {
        return list == null || list.isEmpty();
    }

    @Override
    public void run() {
        try {
            List<String> list = this.client.getChildren().forPath(ZKReentrantLock.ROOT_PATH);
            for (String s : list) {
                cleanNode(s);

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void cleanNode(String s) {
        try {
            if (isEmpty(this.client.getChildren().forPath(s))) {
                this.client.delete().forPath(s);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
