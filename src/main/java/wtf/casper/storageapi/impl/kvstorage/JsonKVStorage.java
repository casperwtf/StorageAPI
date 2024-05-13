package wtf.casper.storageapi.impl.kvstorage;


import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.SneakyThrows;
import wtf.casper.storageapi.KVStorage;
import wtf.casper.storageapi.cache.Cache;
import wtf.casper.storageapi.cache.CaffeineCache;
import wtf.casper.storageapi.id.utils.IdUtils;
import wtf.casper.storageapi.misc.ConstructableValue;
import wtf.casper.storageapi.utils.StorageAPIConstants;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public abstract class JsonKVStorage<K, V> implements KVStorage<K, V>, ConstructableValue<K, V> {

    private final File dataFolder;
    private final Class<K> keyClass;
    private final Class<V> valueClass;
    private Cache<K, V> cache = new CaffeineCache<>(Caffeine.newBuilder().expireAfterWrite(15, TimeUnit.MINUTES).build());

    @SneakyThrows
    public JsonKVStorage(final File dataFolder, final Class<K> keyClass, final Class<V> valueClass) {
        if (dataFolder == null) {
            throw new IllegalArgumentException("Data folder cannot be null");
        }

        if (!dataFolder.exists()) {
            dataFolder.mkdirs();
        }

        if (!dataFolder.isDirectory()) {
            throw new IllegalArgumentException("Data folder must be a directory");
        }

        this.dataFolder = dataFolder;
        this.valueClass = valueClass;
        this.keyClass = keyClass;
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

            File[] files = dataFolder.listFiles();
            if (files == null) {
                return;
            }

            for (File file : files) {
                if (!file.isDirectory()) {
                    file.delete();
                }
            }
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<V> get(K key) {
        return CompletableFuture.supplyAsync(() -> {
            V v = cache.getIfPresent(key);
            if (v != null) {
                return v;
            }

            try {
                File file = new File(dataFolder, key + ".json");

                if (!file.exists()) {
                    return null;
                }

                final Reader reader = new FileReader(file);
                final V value = StorageAPIConstants.getGson().fromJson(reader, valueClass);
                cache.put(key, value);
                reader.close();
                return value;
            } catch (final Exception e) {
                e.printStackTrace();
                return null;
            }

        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Void> save(V value) {
        return CompletableFuture.runAsync(() -> {
            cache.put((K) IdUtils.getId(valueClass, value), value);

            try {
                File file = new File(dataFolder, IdUtils.getId(valueClass, value) + ".json");
                if (!file.exists()) {
                    file.createNewFile();
                } else {
                    file.delete();
                    file.createNewFile();
                }

                final Writer writer = new FileWriter(file);
                StorageAPIConstants.getGson().toJson(value, writer);
                writer.close();
            } catch (final Exception e) {
                e.printStackTrace();
            }

        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Void> remove(V value) {
        return CompletableFuture.runAsync(() -> {
            cache.invalidate((K) IdUtils.getId(valueClass, value));

            final File file = new File(dataFolder, IdUtils.getId(valueClass, value) + ".json");
            if (!file.exists()) {
                return;
            }

            file.delete();
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Void> write() {
        return CompletableFuture.runAsync(() -> {
            cache.asMap().forEach((key, value) -> {
                try {
                    File file = new File(dataFolder, key + ".json");
                    if (!file.exists()) {
                        file.createNewFile();
                    } else {
                        file.delete();
                        file.createNewFile();
                    }

                    final Writer writer = new FileWriter(file);
                    StorageAPIConstants.getGson().toJson(value, writer);
                    writer.close();
                } catch (final Exception e) {
                    e.printStackTrace();
                }
            });
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Collection<V>> allValues() {
        return CompletableFuture.supplyAsync(() -> {
            Collection<V> values = new ArrayList<>();

            File[] files = dataFolder.listFiles();
            if (files == null) {
                return values;
            }

            for (File file : files) {
                if (!file.getName().endsWith(".json")) {
                    continue;
                }

                try {
                    final Reader reader = new FileReader(file);
                    final V value = StorageAPIConstants.getGson().fromJson(reader, valueClass);
                    cache.put((K) IdUtils.getId(valueClass, value), value);
                    reader.close();
                } catch (final Exception e) {
                    e.printStackTrace();
                }
            }

            return cache.asMap().values();
        }, StorageAPIConstants.DB_THREAD_POOL);
    }
}
