package wtf.casper.storageapi.cache;

import lombok.extern.java.Log;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Log
public class MapCache<K, V> implements Cache<K, V> {

    private final Map<K, V> map;

    public MapCache(Object object) {
        if (object instanceof Map) {
            this.map = (Map<K, V>) object;
        } else {
            log.warning("Cache must be a java.util.Map, creating a new ConcurrentHashMap");
            this.map = new ConcurrentHashMap<>();
        }
    }

    @Override
    public V getIfPresent(K key) {
        return map.get(key);
    }

    @Override
    public void put(K key, V value) {
        this.map.put(key, value);
    }

    @Override
    public void putAll(Map<K, V> map) {
        this.map.putAll(map);
    }

    @Override
    public void invalidate(K key) {
        this.map.remove(key);
    }

    @Override
    public void invalidateAll() {
        this.map.clear();
    }

    @Override
    public boolean contains(K key) {
        return this.map.containsKey(key);
    }

    @Override
    public long size() {
        return this.map.size();
    }

    @Override
    public void cleanUp() {
        // No-op
    }

    @Override
    public Map<K, V> asMap() {
        return this.map;
    }
}
