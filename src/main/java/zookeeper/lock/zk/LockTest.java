package zookeeper.lock.zk;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import zookeeper.lock.Callback;
import zookeeper.lock.redis.RedisDistributedLockTemplate;
import zookeeper.lock.redis.RedisLockInternals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

public class LockTest {
    @Test
    public void ZKTry() throws Exception {
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

    @Test
    public void RedisTry() throws InterruptedException {
        JedisPool pool = new JedisPool("127.0.0.1", 6379);
        final RedisDistributedLockTemplate template = new RedisDistributedLockTemplate(pool);

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
                template.execute("lock", 5000, new Callback() {
                    @Override
                    public Object onGetLock() throws InterruptedException {
                        System.out.println(Thread.currentThread().getName() + ": getLock");
                        Thread.sleep(sleepTime);
                        System.out.println(Thread.currentThread().getName() + ": slept for " + sleepTime);
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

    @Test
    public void test() {
        JedisPool pool = new JedisPool("127.0.0.1", 6379);
        Jedis jedis = pool.getResource();
//        RedisLockInternals internals=new RedisLockInternals(pool);
//        String test1 = internals.createRedisKey("test1");
//        System.out.println(test1);
        String luaScript = "" +
                "\nlocal r=tonumber(redis.call('SETNX',KEYS[1],ARGV[1]));" +
                "\nredis.call('PEXPIRE',KEYS[1],ARGV[2]);" +
                "\nreturn r";
        List<String> keys = new ArrayList<>();
        keys.add("abc1234");
        List<String> args = new ArrayList<>();
        args.add("cde");
        args.add("60000");
        jedis.eval(luaScript, keys, args);
        jedis.close();
    }
}
