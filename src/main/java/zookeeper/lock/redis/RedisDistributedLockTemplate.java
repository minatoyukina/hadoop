package zookeeper.lock.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;
import zookeeper.lock.Callback;
import zookeeper.lock.DistributedLockTemplate;

import java.util.concurrent.TimeUnit;

public class RedisDistributedLockTemplate implements DistributedLockTemplate {
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisDistributedLockTemplate.class);
    private JedisPool jedisPool;

    public RedisDistributedLockTemplate(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    @Override
    public Object execute(String lockId, int timeout, Callback callback) {
        RedisDistributedLock lock = null;
        boolean getLock = false;
        try {
            lock = new RedisDistributedLock(jedisPool, lockId);
            if (lock.tryLock((long) timeout, TimeUnit.MILLISECONDS)) {
                getLock = true;
                return callback.onGetLock();
            } else {
                return callback.onTimeout();
            }
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            if (getLock) {
                lock.unlock();
            }
        }
        return null;
    }
}
