package wtf.casper.storageapi;

import wtf.casper.storageapi.cache.Cache;

public interface KVStorage<K, V> extends StatelessKVStorage<K, V>{

    /**
     * @return the cache used by this storage.
     */
    Cache<K, V> cache();

    /**
     * @param cache the new cache to use.
     */
    void cache(Cache<K, V> cache);

}
