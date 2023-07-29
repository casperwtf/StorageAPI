package wtf.casper.storageapi.cache;

import java.util.Map;

public interface Cache<K, V> {

    V getIfPresent(K key);

    void put(K key, V value);

    void putAll(Map<K, V> map);

    void invalidate(K key);

    void invalidateAll();

    boolean contains(K key);

    long size();

    void cleanUp();

    Map<K, V> asMap();
}
