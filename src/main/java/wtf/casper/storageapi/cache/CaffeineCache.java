package wtf.casper.storageapi.cache;

import java.util.Map;

public class CaffeineCache<K, V> implements Cache<K, V> {

    private final com.github.benmanes.caffeine.cache.Cache<K, V> cache;

    public CaffeineCache(Object cache) {
        if (cache instanceof com.github.benmanes.caffeine.cache.Cache) {
            this.cache = (com.github.benmanes.caffeine.cache.Cache<K, V>) cache;
        } else {
            throw new IllegalArgumentException("Cache must be a com.github.benmanes.caffeine.cache.Cache");
        }
    }

    @Override
    public V getIfPresent(K key) {
        return cache.getIfPresent(key);
    }

    @Override
    public void put(K key, V value) {
        cache.put(key, value);
    }

    @Override
    public void putAll(Map<K, V> map) {
        cache.putAll(map);
    }

    @Override
    public void invalidate(K key) {
        cache.invalidate(key);
    }

    @Override
    public void invalidateAll() {
        cache.invalidateAll();
    }

    @Override
    public boolean contains(K key) {
        return cache.getIfPresent(key) != null;
    }

    @Override
    public long size() {
        return cache.estimatedSize();
    }

    @Override
    public void cleanUp() {
        cache.cleanUp();
    }

    @Override
    public Map<K, V> asMap() {
        return cache.asMap();
    }
}
