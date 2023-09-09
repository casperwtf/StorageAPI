package wtf.casper.storageapi.impl.statelesskvstorage;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import lombok.Getter;
import lombok.extern.java.Log;
import org.bson.Document;
import wtf.casper.storageapi.*;
import wtf.casper.storageapi.id.utils.IdUtils;
import wtf.casper.storageapi.misc.ConstructableValue;
import wtf.casper.storageapi.misc.IMongoStorage;
import wtf.casper.storageapi.misc.MongoProvider;
import wtf.casper.storageapi.utils.Constants;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Log
public class StatelessMongoKVStorage<K, V> implements StatelessKVStorage<K, V>, ConstructableValue<K, V>, IMongoStorage {

    private final Class<K> keyClass;
    private final Class<V> valueClass;
    private final String idFieldName;
    private final MongoClient mongoClient;
    @Getter
    private final MongoCollection<Document> collection;

    public StatelessMongoKVStorage(final Class<K> keyClass, final Class<V> valueClass, final Credentials credentials) {
        this(credentials.getUri(), credentials.getDatabase(), credentials.getCollection(), keyClass, valueClass);
    }

    public StatelessMongoKVStorage(final String uri, final String database, final String collection, final Class<K> keyClass, final Class<V> valueClass) {
        this.valueClass = valueClass;
        this.keyClass = keyClass;
        this.idFieldName = IdUtils.getIdName(this.valueClass);
        try {
            log.fine("Connecting to MongoDB...");
            mongoClient = MongoProvider.getClient(uri);
        } catch (Exception e) {
            log.warning("\n\n");
            log.warning("Failed to connect to MongoDB. Please check your credentials.");
            log.warning("\n\n");
            log.warning("Developer Stack Trace: ");
            log.warning(" ");
            throw e;
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
        });
    }

    @Override
    public CompletableFuture<V> get(K key) {
        return CompletableFuture.supplyAsync(() -> {
            Document filter = new Document(idFieldName, convertUUIDtoString(key));
            Document document = getCollection().find(filter).first();

            if (document == null) {
                return null;
            }

            return Constants.getGson().fromJson(document.toJson(Constants.getJsonWriterSettings()), valueClass);
        });
    }

    @Override
    public CompletableFuture<Void> save(V value) {
        return CompletableFuture.runAsync(() -> {
            K key = (K) IdUtils.getId(valueClass, value);
            getCollection().replaceOne(
                    new Document(idFieldName, convertUUIDtoString(key)),
                    Document.parse(Constants.getGson().toJson(value)),
                    new ReplaceOptions().upsert(true)
            );
        });
    }

    @Override
    public CompletableFuture<Void> remove(V key) {
        return CompletableFuture.runAsync(() -> {
            try {
                K id = (K) IdUtils.getId(valueClass, key);
                getCollection().deleteMany(getDocument(FilterType.EQUALS, idFieldName, convertUUIDtoString(id)));
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public CompletableFuture<Void> write() {
        // No need to write to mongo
        return CompletableFuture.runAsync(() -> {
        });
    }

    @Override
    public CompletableFuture<Void> close() {
        // No need to close mongo because it's handled by a provider
        return CompletableFuture.runAsync(() -> {
        });
    }

    @Override
    public CompletableFuture<Collection<V>> allValues() {
        return CompletableFuture.supplyAsync(() -> {
            List<Document> into = getCollection().find().into(new ArrayList<>());
            List<V> collection = new ArrayList<>();

            for (Document document : into) {
                V obj = Constants.getGson().fromJson(document.toJson(Constants.getJsonWriterSettings()), valueClass);
                collection.add(obj);
            }

            return collection;
        });
    }
}
