package zookeeper.lock.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

class RedisLockInternals {
    private JedisPool jedisPool;
    private int retryAwait = 300;
    private int lockTimeout = 2000;

    RedisLockInternals(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    String tryRedisLock(String lockId, long time, TimeUnit unit) {
        final long startMillis = System.currentTimeMillis();
        final long millisToWait = unit.toMillis(time);
        String lockValue;
        while (true) {
            lockValue = createRedisKey(lockId);
            if (lockValue != null) {
                break;
            }
            if (System.currentTimeMillis() - startMillis - retryAwait > millisToWait) {
                break;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(retryAwait));
        }
        return lockValue;
    }

    private String createRedisKey(String lockId) {
        Jedis jedis = null;
        try {
            String value = lockId + randomId(1);
            jedis = jedisPool.getResource();
            String luaScript = "" +
                    "\nlocal r=tonumber(redis.call('SETNX',KEYS[1],ARGV[1]));" +
                    "\nredis.call('PEXPIRE',KEYS[1],ARGV[2]);" +
                    "\nreturn r";
            List<String> keys = new ArrayList<>();
            keys.add(lockId);
            List<String> args = new ArrayList<>();
            args.add(value);
            args.add(lockTimeout + "");
            Long ret = (Long) jedis.eval(luaScript, keys, args);
            if (new Long(1).equals(ret)) {
                return value;
            }
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return null;
    }

    private final static char[] digits = {'0', '1', '2', '3', '4', '5', '6', '7', '8',
            '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l',
            'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y',
            'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L',
            'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y',
            'Z'};

    private String randomId(int size) {
        char[] cs = new char[size];
        for (int i = 0; i < cs.length; i++) {
            cs[i] = digits[ThreadLocalRandom.current().nextInt(digits.length)];
        }
        return new String(cs);
    }

    void unlockRedisLock(String key, String value) {
        try (Jedis jedis = jedisPool.getResource()) {
            String luaScript = "" +
                    "\nlocal v=redis.call('GET',KEYS[1]);" +
                    "\nlocal r=0;" +
                    "\nif v==ARGV[1] then" +
                    "\nr=redis.call('DEL',KEYS[1]);" +
                    "\nend" +
                    "\nreturn r";
            List<String> keys = new ArrayList<>();
            keys.add(key);
            List<String> args = new ArrayList<>();
            args.add(value);
            jedis.eval(luaScript, keys, args);
        }
    }

}
