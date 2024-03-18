package wtf.casper.storageapi.impl.kvstorage;


import lombok.SneakyThrows;
import wtf.casper.storageapi.KVStorage;
import wtf.casper.storageapi.cache.Cache;
import wtf.casper.storageapi.cache.MapCache;
import wtf.casper.storageapi.id.utils.IdUtils;
import wtf.casper.storageapi.misc.ConstructableValue;
import wtf.casper.storageapi.utils.Constants;

import java.io.*;
import java.lang.reflect.Array;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

public abstract class JsonKVStorage<K, V> implements KVStorage<K, V>, ConstructableValue<K, V> {

    private final File file;
    private final Class<K> keyClass;
    private final Class<V> valueClass;
    private Cache<K, V> cache = new MapCache<>(new HashMap<>());

    @SneakyThrows
    public JsonKVStorage(final File file, final Class<K> keyClass, final Class<V> valueClass) {
        this.file = file;
        this.valueClass = valueClass;
        this.keyClass = keyClass;

        if (!file.exists()) {
            file.getParentFile().mkdirs();
            return;
        }

        FileReader json = new FileReader(file);
        final V[] values = Constants.getGson().fromJson(json, (Class<V[]>) Array.newInstance(valueClass, 0).getClass());
        json.close();

        if (values != null) {
            for (final V value : values) {
                this.cache.put((K) IdUtils.getId(valueClass, value), value);
            }
        }
    }

    @Override
    public Class<K> key() {
        return keyClass;
    }

    @Override
    public Class<V> value() {
        return valueClass;
    }

    @Override
    public Cache<K, V> cache() {
        return cache;
    }

    @Override
    public void cache(Cache<K, V> cache) {
        this.cache = cache;
    }

    @Override
    public CompletableFuture<Void> deleteAll() {
        return CompletableFuture.runAsync(() -> {
            this.cache.invalidateAll();
        }, Constants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<V> get(K key) {
        return CompletableFuture.supplyAsync(() -> {
            return cache.getIfPresent(key);
        }, Constants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Void> save(V value) {
        return CompletableFuture.runAsync(() -> {
            cache.put((K) IdUtils.getId(valueClass, value), value);
        }, Constants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Void> remove(V value) {
        return CompletableFuture.runAsync(() -> {
            cache.invalidate((K) IdUtils.getId(valueClass, value));
        }, Constants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Void> write() {
        return CompletableFuture.runAsync(() -> {
            try {
                boolean delete = this.file.delete();

                if (!delete) {
                    System.out.println("Failed to delete file " + this.file.getAbsolutePath());
                }

                this.file.createNewFile();

                final Writer writer = new FileWriter(this.file);

                Constants.getGson().toJson(this.cache.asMap().values(), writer);
                writer.close();
            } catch (final Exception e) {
                e.printStackTrace();
            }
        }, Constants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Collection<V>> allValues() {
        return CompletableFuture.supplyAsync(() -> cache.asMap().values(), Constants.DB_THREAD_POOL);
    }
}
