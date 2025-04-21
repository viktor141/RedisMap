package org.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisException;

import java.util.*;

public class RedisMap implements Map<String, String> {
    private final JedisPool jedisPool;
    private final String mapKey;

    public RedisMap(String host, int port, String mapKey) {
        if (host == null || mapKey == null) {
            throw new NullPointerException("Host and mapKey cannot be null");
        }
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("Port must be between 1 and 65535");
        }
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
        } catch (JedisException e) {
            throw new RuntimeException("Failed to get size from Redis", e);
        }
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        if (key == null) {
            throw new NullPointerException("Key cannot be null");
        }
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.hexists(mapKey, key.toString());
        } catch (JedisException e) {
            throw new RuntimeException("Failed to check key in Redis", e);
        }
    }

    @Override
    public boolean containsValue(Object value) {
        if (value == null) {
            throw new NullPointerException("Value cannot be null");
        }
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.hvals(mapKey).contains(value.toString());
        } catch (JedisException e) {
            throw new RuntimeException("Failed to check value in Redis", e);
        }
    }

    @Override
    public String get(Object key) {
        if (key == null) {
            throw new NullPointerException("Key cannot be null");
        }
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.hget(mapKey, key.toString());
        } catch (JedisException e) {
            throw new RuntimeException("Failed to get value from Redis", e);
        }
    }

    @Override
    public String put(String key, String value) {
        if (key == null || value == null) {
            throw new NullPointerException("Key or value cannot be null");
        }
        try (Jedis jedis = jedisPool.getResource()) {
            String oldValue = jedis.hget(mapKey, key);
            jedis.hset(mapKey, key, value);
            return oldValue;
        } catch (JedisException e) {
            throw new RuntimeException("Failed to put value in Redis", e);
        }
    }

    @Override
    public String remove(Object key) {
        if (key == null) {
            throw new NullPointerException("Key cannot be null");
        }
        try (Jedis jedis = jedisPool.getResource()) {
            String value = jedis.hget(mapKey, key.toString());
            jedis.hdel(mapKey, key.toString());
            return value;
        } catch (JedisException e) {
            throw new RuntimeException("Failed to remove value from Redis", e);
        }
    }

    @Override
    public void putAll(Map<? extends String, ? extends String> m) {
        if (m == null) {
            throw new NullPointerException("Map cannot be null");
        }
        for (Entry<? extends String, ? extends String> entry : m.entrySet()) {
            if (entry.getKey() == null || entry.getValue() == null) {
                throw new NullPointerException("Map contains null key or value");
            }
        }
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.hmset(mapKey, new HashMap<>(m));
        } catch (JedisException e) {
            throw new RuntimeException("Failed to put all values in Redis", e);
        }
    }

    @Override
    public void clear() {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(mapKey);
        } catch (JedisException e) {
            throw new RuntimeException("Failed to clear Redis map", e);
        }
    }

    @Override
    public Set<String> keySet() {
        try (Jedis jedis = jedisPool.getResource()) {
            return new HashSet<>(jedis.hkeys(mapKey));
        } catch (JedisException e) {
            throw new RuntimeException("Failed to get keys from Redis", e);
        }
    }

    @Override
    public Collection<String> values() {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.hvals(mapKey);
        } catch (JedisException e) {
            throw new RuntimeException("Failed to get values from Redis", e);
        }
    }

    @Override
    public Set<Entry<String, String>> entrySet() {
        try (Jedis jedis = jedisPool.getResource()) {
            Map<String, String> map = jedis.hgetAll(mapKey);
            return map.entrySet();
        } catch (JedisException e) {
            throw new RuntimeException("Failed to get entries from Redis", e);
        }
    }
}
