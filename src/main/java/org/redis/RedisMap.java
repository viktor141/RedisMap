package org.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.*;

public class RedisMap implements Map<String, String> {
    private final JedisPool jedisPool;
    private final String mapKey;

    public RedisMap(String host, int port, String mapKey) {
        this.jedisPool = new JedisPool(new JedisPoolConfig(), host, port);
        this.mapKey = mapKey;
    }

    @Override
    public int size() {
        try (Jedis jedis = jedisPool.getResource()) {
            long size = jedis.hlen(mapKey);
            if (size > Integer.MAX_VALUE) {
                throw new ArithmeticException("Size exceeds Integer.MAX_VALUE");
            }
            return (int) size;
        }
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        if (key == null) return false;
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.hexists(mapKey, key.toString());
        }
    }

    @Override
    public boolean containsValue(Object value) {
        if (value == null) return false;
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.hvals(mapKey).contains(value.toString());
        }
    }

    @Override
    public String get(Object key) {
        if (key == null) return null;
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.hget(mapKey, key.toString());
        }
    }

    @Override
    public String put(String key, String value) {
        if (key == null || value == null) throw new NullPointerException("Key or value cannot be null");
        try (Jedis jedis = jedisPool.getResource()) {
            String oldValue = jedis.hget(mapKey, key);
            jedis.hset(mapKey, key, value);
            return oldValue;
        }
    }

    @Override
    public String remove(Object key) {
        if (key == null) return null;
        try (Jedis jedis = jedisPool.getResource()) {
            String value = jedis.hget(mapKey, key.toString());
            jedis.hdel(mapKey, key.toString());
            return value;
        }
    }

    @Override
    public void putAll(Map<? extends String, ? extends String> m) {
        if (m == null) return;
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.hmset(mapKey, new HashMap<>(m));
        }
    }

    @Override
    public void clear() {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(mapKey);
        }
    }

    @Override
    public Set<String> keySet() {
        try (Jedis jedis = jedisPool.getResource()) {
            return new HashSet<>(jedis.hkeys(mapKey));
        }
    }

    @Override
    public Collection<String> values() {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.hvals(mapKey);
        }
    }

    @Override
    public Set<Entry<String, String>> entrySet() {
        try (Jedis jedis = jedisPool.getResource()) {
            Map<String, String> map = jedis.hgetAll(mapKey);
            return map.entrySet();
        }
    }
}
