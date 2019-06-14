package zookeeper.lock.zk;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Test;
import zookeeper.lock.Callback;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

public class ZKLockTest {
    @Test
    public void testTry() throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient("db1:2181,db2:2181,db3:2181", retryPolicy);
        client.start();
        final ZKDistributedLockTemplate template = new ZKDistributedLockTemplate(client);
        int size = 100;
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch endLatch = new CountDownLatch(size);
        for (int i = 0; i < size; i++) {
            new Thread(() -> {
                try {
                    startLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                final int sleepTime = ThreadLocalRandom.current().nextInt(5) * 1000;
                template.execute("test", 5000, new Callback() {
                    @Override
                    public Object onGetLock() throws InterruptedException {
                        System.out.println(Thread.currentThread().getName() + ": getLock");
                        Thread.sleep(sleepTime);
                        System.out.println(Thread.currentThread().getName() + ": slept " + sleepTime);
                        endLatch.countDown();
                        return null;
                    }

                    @Override
                    public Object onTimeout() {
                        System.out.println(Thread.currentThread().getName() + ": timeout");
                        endLatch.countDown();
                        return null;
                    }
                });

            }).start();
        }
        startLatch.countDown();
        endLatch.await();

    }
}
