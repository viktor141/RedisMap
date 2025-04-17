import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.redis.RedisMap;
import redis.clients.jedis.Jedis;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class RedisMapTest {
    private static RedisMap map;

    @BeforeAll
    static void setUp() {
        map = new RedisMap("localhost", 6379, "testMap");
        map.clear();
    }

    @AfterAll
    static void tearDown() {
        map.clear();
    }

    @AfterEach
    void cleanUp() {
        try (Jedis jedis = new Jedis("localhost", 6379)) {
            jedis.del("testMap");
        }
    }

    @Test
    void testPutAndGet() {
        map.put("key1", "value1");
        assertEquals("value1", map.get("key1"));
        assertNull(map.get("key2"));
    }

    @Test
    void testPutNull() {
        assertThrows(NullPointerException.class, () -> map.put(null, "value"));
        assertThrows(NullPointerException.class, () -> map.put("key", null));
    }

    @Test
    void testRemove() {
        map.put("key1", "value1");
        assertEquals("value1", map.remove("key1"));
        assertNull(map.get("key1"));
        assertNull(map.remove("key2"));
    }

    @Test
    void testContainsKey() {
        map.put("key1", "value1");
        assertTrue(map.containsKey("key1"));
        assertFalse(map.containsKey("key2"));
        assertFalse(map.containsKey(null));
    }

    @Test
    void testContainsValue() {
        map.put("key1", "value1");
        assertTrue(map.containsValue("value1"));
        assertFalse(map.containsValue("value2"));
        assertFalse(map.containsValue(null));
    }

    @Test
    void testSizeAndIsEmpty() {
        assertTrue(map.isEmpty());
        assertEquals(0, map.size());
        map.put("key1", "value1");
        assertFalse(map.isEmpty());
        assertEquals(1, map.size());
    }

    @Test
    void testClear() {
        map.put("key1", "value1");
        map.put("key2", "value2");
        map.clear();
        assertTrue(map.isEmpty());
        assertEquals(0, map.size());
        assertNull(map.get("key1"));
    }

    @Test
    void testPutAll() {
        Map<String, String> data = new HashMap<>();
        data.put("key1", "value1");
        data.put("key2", "value2");
        map.putAll(data);
        assertEquals("value1", map.get("key1"));
        assertEquals("value2", map.get("key2"));
        assertEquals(2, map.size());
    }

    @Test
    void testKeySet() {
        map.put("key1", "value1");
        map.put("key2", "value2");
        Set<String> keys = map.keySet();
        assertEquals(2, keys.size());
        assertTrue(keys.contains("key1"));
        assertTrue(keys.contains("key2"));
    }

    @Test
    void testValues() {
        map.put("key1", "value1");
        map.put("key2", "value2");
        Collection<String> values = map.values();
        assertEquals(2, values.size());
        assertTrue(values.contains("value1"));
        assertTrue(values.contains("value2"));
    }

    @Test
    void testEntrySet() {
        map.put("key1", "value1");
        map.put("key2", "value2");
        Set<Map.Entry<String, String>> entries = map.entrySet();
        assertEquals(2, entries.size());
        assertTrue(entries.contains(new AbstractMap.SimpleEntry<>("key1", "value1")));
        assertTrue(entries.contains(new AbstractMap.SimpleEntry<>("key2", "value2")));
    }


}
