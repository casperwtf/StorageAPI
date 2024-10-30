package wtf.casper.storageapi.impl.fstorage;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import lombok.Getter;
import lombok.extern.java.Log;
import org.bson.Document;
import org.bson.conversions.Bson;
import wtf.casper.storageapi.*;
import wtf.casper.storageapi.cache.Cache;
import wtf.casper.storageapi.cache.CaffeineCache;
import wtf.casper.storageapi.id.utils.IdUtils;
import wtf.casper.storageapi.impl.statelessfstorage.StatelessMongoFStorage;
import wtf.casper.storageapi.misc.ConstructableValue;
import wtf.casper.storageapi.misc.IMongoStorage;
import wtf.casper.storageapi.misc.MongoProvider;
import wtf.casper.storageapi.utils.StorageAPIConstants;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Log
public abstract class MongoFStorage<K, V> extends StatelessMongoFStorage<K, V> implements FieldStorage<K, V>, ConstructableValue<K, V>, IMongoStorage {

    private Cache<K, V> cache = new CaffeineCache<>(Caffeine.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES).build());

    public MongoFStorage(final Class<K> keyClass, final Class<V> valueClass, final Credentials credentials) {
        this(credentials.getUri(), credentials.getDatabase(), credentials.getCollection(), keyClass, valueClass);
    }

    public MongoFStorage(final String uri, final String database, final String collection, final Class<K> keyClass, final Class<V> valueClass) {
        super(uri, database, collection, keyClass, valueClass);
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
    public CompletableFuture<Collection<V>> get(String field, Object value, FilterType filterType, SortingType sortingType) {
        return CompletableFuture.supplyAsync(() -> {

            Collection<V> collection = new ArrayList<>();
            Bson filter = getDocument(filterType, field, value);
            List<Document> into = getCollection().find(filter).into(new ArrayList<>());

            for (Document document : into) {
                V obj = StorageAPIConstants.getGson().fromJson(document.toJson(StorageAPIConstants.getJsonWriterSettings()), valueClass);
                collection.add(obj);
            }

            return sortingType.sort(collection, field);
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<V> get(K key) {
        return CompletableFuture.supplyAsync(() -> {
            if (cache.getIfPresent(key) != null) {
                return cache.getIfPresent(key);
            }
            V join = super.get(key).join();
            cache.asMap().putIfAbsent(key, join);
            return join;
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<V> getFirst(String field, Object value, FilterType filterType) {
        return CompletableFuture.supplyAsync(() -> {
            if (!cache.asMap().isEmpty()) {
                for (V v : cache.asMap().values()) {
                    if (filterType.passes(v, field, value)) {
                        return v;
                    }
                }
            }

            V obj = super.getFirst(field, value, filterType).join();
            cache.asMap().putIfAbsent((K) IdUtils.getId(obj), obj);

            return obj;
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Void> save(V value) {
        return CompletableFuture.runAsync(() -> {
            K key = (K) IdUtils.getId(valueClass, value);
            cache.asMap().putIfAbsent(key, value);
            super.save(value).join();
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Void> saveAll(Collection<V> values) {
        return CompletableFuture.runAsync(() -> {
            for (V value : values) {
                K key = (K) IdUtils.getId(valueClass, value);
                cache.asMap().putIfAbsent(key, value);
            }
            super.saveAll(values).join();
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Void> remove(V key) {
        return CompletableFuture.runAsync(() -> {
            try {
                K id = (K) IdUtils.getId(valueClass, key);
                cache.invalidate(id);
                getCollection().deleteMany(getDocument(FilterType.EQUALS, "_id", convertUUIDtoString(id)));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Void> close() {
        return CompletableFuture.supplyAsync(() -> {
            cache.invalidateAll();
            MongoProvider.closeClient(uri);
            return null;
        }, StorageAPIConstants.DB_THREAD_POOL);
    }
}
