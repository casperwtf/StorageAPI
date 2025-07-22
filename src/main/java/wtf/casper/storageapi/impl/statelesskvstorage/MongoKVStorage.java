package wtf.casper.storageapi.impl.statelesskvstorage;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import lombok.Getter;
import lombok.extern.java.Log;
import org.bson.Document;
import wtf.casper.storageapi.Credentials;
import wtf.casper.storageapi.id.utils.IdUtils;
import wtf.casper.storageapi.misc.ConstructableValue;
import wtf.casper.storageapi.misc.MongoStorage;
import wtf.casper.storageapi.misc.MongoProvider;
import wtf.casper.storageapi.utils.StorageAPIConstants;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Log
public class MongoKVStorage<K, V> implements wtf.casper.storageapi.KeyedStorage<K, V>, ConstructableValue<K, V>, MongoStorage {

    private final Class<K> keyClass;
    private final Class<V> valueClass;
    private final String idFieldName;
    @Getter
    private final MongoCollection<Document> collection;
    private final ReplaceOptions replaceOptions = new ReplaceOptions().upsert(true);

    public MongoKVStorage(final Class<K> keyClass, final Class<V> valueClass, final Credentials credentials) {
        this(credentials.getUri(), credentials.getDatabase(), credentials.getCollection(), keyClass, valueClass);
    }

    public MongoKVStorage(final String uri, final String database, final String collection, final Class<K> keyClass, final Class<V> valueClass) {
        this.valueClass = valueClass;
        this.keyClass = keyClass;
        this.idFieldName = IdUtils.getIdName(this.valueClass);
        MongoClient mongoClient;
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
    public Class<K> key() {
        return keyClass;
    }

    @Override
    public Class<V> value() {
        return valueClass;
    }

    @Override
    public CompletableFuture<Void> deleteAll() {
        return CompletableFuture.runAsync(() -> {
            getCollection().deleteMany(new Document());
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<V> get(K key) {
        return CompletableFuture.supplyAsync(() -> {
            Document filter = new Document(idFieldName, convertUUIDtoString(key));
            Document document = getCollection().find(filter).first();

            if (document == null) {
                return null;
            }

            return StorageAPIConstants.getGson().fromJson(document.toJson(StorageAPIConstants.getJsonWriterSettings()), valueClass);
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Void> save(V value) {
        return CompletableFuture.runAsync(() -> {
            K key = (K) IdUtils.getId(valueClass, value);
            getCollection().replaceOne(
                    new Document("_id", convertUUIDtoString(key)),
                    Document.parse(StorageAPIConstants.getGson().toJson(value)),
                    replaceOptions
            );
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Void> saveAll(Collection<V> values) {
        return CompletableFuture.runAsync(() -> {
            for (V value : values) {
                K key = (K) IdUtils.getId(valueClass, value);
                getCollection().replaceOne(
                        new Document("_id", convertUUIDtoString(key)),
                        Document.parse(StorageAPIConstants.getGson().toJson(value)),
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
                getCollection().deleteMany(new Document(idFieldName, convertUUIDtoString(id)));
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

    @Override
    public CompletableFuture<Void> renameFields(Map<String, String> pathToNewPath) {
        return CompletableFuture.runAsync(() -> {
            for (Map.Entry<String, String> entry : pathToNewPath.entrySet()) {
                getCollection().updateMany(
                        new Document(),
                        new Document("$rename", new Document(entry.getKey(), entry.getValue()))
                );
            }
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Void> renameField(String path, String newPath) {
        return CompletableFuture.runAsync(() -> {
            getCollection().updateMany(
                    new Document(),
                    new Document("$rename", new Document(path, newPath))
            );
        }, StorageAPIConstants.DB_THREAD_POOL);
    }
}
