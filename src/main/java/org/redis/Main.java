package org.redis;

public class Main {
    public static void main(String[] args) {
        RedisMap map = new RedisMap("localhost", 6379, "myMap");
        map.put("key1", "value1");
        System.out.println(map.get("key1"));
    }
}