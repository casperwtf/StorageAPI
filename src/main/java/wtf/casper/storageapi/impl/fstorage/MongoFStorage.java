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
import wtf.casper.storageapi.Credentials;
import wtf.casper.storageapi.FieldStorage;
import wtf.casper.storageapi.FilterType;
import wtf.casper.storageapi.SortingType;
import wtf.casper.storageapi.cache.Cache;
import wtf.casper.storageapi.cache.CaffeineCache;
import wtf.casper.storageapi.id.utils.IdUtils;
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
public class MongoFStorage<K, V> implements FieldStorage<K, V>, ConstructableValue<K, V>, IMongoStorage {

    protected final Class<K> keyClass;
    protected final Class<V> valueClass;
    private final String idFieldName;
    private final MongoClient mongoClient;
    @Getter
    private final MongoCollection<Document> collection;
    private final ReplaceOptions replaceOptions = new ReplaceOptions().upsert(true);

    private Cache<K, V> cache = new CaffeineCache<>(Caffeine.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES).build());

    public MongoFStorage(final Class<K> keyClass, final Class<V> valueClass, final Credentials credentials) {
        this(credentials.getUri(), credentials.getDatabase(), credentials.getCollection(), keyClass, valueClass);
    }

    public MongoFStorage(final String uri, final String database, final String collection, final Class<K> keyClass, final Class<V> valueClass) {
        this.valueClass = valueClass;
        this.keyClass = keyClass;
        this.idFieldName = IdUtils.getIdName(this.valueClass);
        try {
            mongoClient = MongoProvider.getClient(uri);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to connect to mongo");
        }

        MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
        this.collection = mongoDatabase.getCollection(collection);
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
    public Class<V> value() {
        return valueClass;
    }

    @Override
    public Class<K> key() {
        return keyClass;
    }

    @Override
    public CompletableFuture<Void> deleteAll() {
        return CompletableFuture.runAsync(() -> {
            getCollection().deleteMany(new Document());
        }, StorageAPIConstants.DB_THREAD_POOL);
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

            Document filter = new Document("_id", convertUUIDtoString(key));
            Document document = getCollection().find(filter).first();

            if (document == null) {
                return null;
            }

            V obj = StorageAPIConstants.getGson().fromJson(document.toJson(StorageAPIConstants.getJsonWriterSettings()), valueClass);
            cache.asMap().putIfAbsent(key, obj);
            return obj;
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

            Bson filter = getDocument(filterType, field, value);
            Document document = getCollection().find(filter).first();

            if (document == null) {
                return null;
            }

            V obj = StorageAPIConstants.getGson().fromJson(document.toJson(StorageAPIConstants.getJsonWriterSettings()), valueClass);
            K key = (K) IdUtils.getId(valueClass, obj);
            cache.asMap().putIfAbsent(key, obj);
            return obj;
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Void> save(V value) {
        return CompletableFuture.runAsync(() -> {
            K key = (K) IdUtils.getId(valueClass, value);
            cache.asMap().putIfAbsent(key, value);
            Document document = Document.parse(StorageAPIConstants.getGson().toJson(value));
            document.put("_id", convertUUIDtoString(key));
            getCollection().replaceOne(
                    new Document(idFieldName, convertUUIDtoString(key)),
                    document,
                    replaceOptions
            );
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Void> saveAll(Collection<V> values) {
        return CompletableFuture.runAsync(() -> {
            for (V value : values) {
                K key = (K) IdUtils.getId(valueClass, value);
                cache.asMap().putIfAbsent(key, value);
                Document document = Document.parse(StorageAPIConstants.getGson().toJson(value));
                document.put("_id", convertUUIDtoString(key));
                getCollection().replaceOne(
                        new Document(idFieldName, convertUUIDtoString(key)),
                        document,
                        replaceOptions
                );
            }
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
    public CompletableFuture<Void> write() {
        // No need to write to mongo
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> close() {
        // No need to close mongo because it's handled by a provider
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Collection<V>> allValues() {
        return CompletableFuture.supplyAsync(() -> {
            List<Document> into = getCollection().find().into(new ArrayList<>());
            List<V> collection = new ArrayList<>();

            for (Document document : into) {
                V obj = StorageAPIConstants.getGson().fromJson(document.toJson(StorageAPIConstants.getJsonWriterSettings()), valueClass);
                collection.add(obj);
            }

            return collection;
        }, StorageAPIConstants.DB_THREAD_POOL);
    }
}
