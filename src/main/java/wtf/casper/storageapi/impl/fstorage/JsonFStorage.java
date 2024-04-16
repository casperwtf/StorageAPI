package wtf.casper.storageapi.impl.fstorage;


import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.SneakyThrows;
import org.jetbrains.annotations.Nullable;
import wtf.casper.storageapi.FieldStorage;
import wtf.casper.storageapi.FilterType;
import wtf.casper.storageapi.SortingType;
import wtf.casper.storageapi.cache.Cache;
import wtf.casper.storageapi.cache.CaffeineCache;
import wtf.casper.storageapi.cache.MapCache;
import wtf.casper.storageapi.id.utils.IdUtils;
import wtf.casper.storageapi.misc.ConstructableValue;
import wtf.casper.storageapi.utils.StorageAPIConstants;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.Writer;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public abstract class JsonFStorage<K, V> implements FieldStorage<K, V>, ConstructableValue<K, V> {

    private final File file;
    private final Class<K> keyClass;
    private final Class<V> valueClass;
    private Cache<K, V> cache = new MapCache<>(new HashMap<>());

    @SneakyThrows
    public JsonFStorage(final File file, final Class<K> keyClass, final Class<V> valueClass) {
        if (file == null) {
            throw new IllegalArgumentException("File cannot be null");
        }

        if (!file.exists()) {
            file.getParentFile().mkdirs();
            if (!file.createNewFile()) {
                throw new RuntimeException("Failed to create file " + file.getAbsolutePath());
            }
        }

        this.file = file;
        this.valueClass = valueClass;
        this.keyClass = keyClass;

        final V[] values = StorageAPIConstants.getGson().fromJson(new FileReader(file), (Class<V[]>) Array.newInstance(valueClass, 0).getClass());

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
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Collection<V>> get(String field, Object value, FilterType filterType, SortingType sortingType) {
        return CompletableFuture.supplyAsync(() -> {
            Collection<V> values = cache().asMap().values();
            return sortingType.sort(filter(values, field, value, filterType), field);
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<V> get(K key) {
        return CompletableFuture.supplyAsync(() -> cache.getIfPresent(key));
    }

    @Override
    public CompletableFuture<V> getFirst(String field, Object value, FilterType filterType) {
        return CompletableFuture.supplyAsync(() -> {
            Collection<V> values = cache().asMap().values();
            return filterFirst(values, field, value, filterType);
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Void> save(V value) {
        return CompletableFuture.runAsync(() -> {
            cache.put((K) IdUtils.getId(valueClass, value), value);
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Void> remove(V value) {
        return CompletableFuture.runAsync(() -> {
            cache.invalidate((K) IdUtils.getId(valueClass, value));
        }, StorageAPIConstants.DB_THREAD_POOL);
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

                StorageAPIConstants.getGson().toJson(this.cache.asMap().values(), writer);
                writer.close();
            } catch (final Exception e) {
                e.printStackTrace();
            }
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Collection<V>> allValues() {
        return CompletableFuture.supplyAsync(() -> cache.asMap().values(), StorageAPIConstants.DB_THREAD_POOL);
    }

    private Collection<V> filter(final Collection<V> values, final String field, final Object value, FilterType filterType) {
        List<V> list = new ArrayList<>();
        for (final V v : values) {
            if (filterType.passes(v, field, value)) {
                list.add(v);
            }
        }
        return list;
    }

    @Nullable
    private V filterFirst(final Collection<V> values, final String field, final Object value, FilterType filterType) {
        for (final V v : values) {
            if (filterType.passes(v, field, value)) return v;
        }
        return null;
    }
}
