package wtf.casper.storageapi.impl.statelessfstorage;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import lombok.Getter;
import lombok.extern.java.Log;
import org.bson.Document;
import org.bson.conversions.Bson;
import wtf.casper.storageapi.Credentials;
import wtf.casper.storageapi.FilterType;
import wtf.casper.storageapi.SortingType;
import wtf.casper.storageapi.StatelessFieldStorage;
import wtf.casper.storageapi.id.utils.IdUtils;
import wtf.casper.storageapi.misc.ConstructableValue;
import wtf.casper.storageapi.misc.IMongoStorage;
import wtf.casper.storageapi.misc.MongoProvider;
import wtf.casper.storageapi.misc.StorageGson;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Log
public class StatelessMongoFStorage<K, V> implements StatelessFieldStorage<K, V>, ConstructableValue<K, V>, IMongoStorage {

    private final Class<V> type;
    private final String idFieldName;
    private final MongoClient mongoClient;
    @Getter
    private final MongoCollection<Document> collection;

    public StatelessMongoFStorage(final Class<V> type, final Credentials credentials) {
        this(credentials.getUri(), credentials.getDatabase(), credentials.getCollection(), type);
    }

    public StatelessMongoFStorage(final String uri, final String database, final String collection, final Class<V> type) {
        this.type = type;
        this.idFieldName = IdUtils.getIdName(this.type);
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
    public CompletableFuture<Collection<V>> get(String field, Object value, FilterType filterType, SortingType sortingType) {
        return CompletableFuture.supplyAsync(() -> {

            Collection<V> collection = new ArrayList<>();
            Bson filter = getDocument(filterType, field, value);
            List<Document> into = getCollection().find(filter).into(new ArrayList<>());

            for (Document document : into) {
                V obj = StorageGson.getGson().fromJson(document.toJson(StorageGson.getJsonWriterSettings()), type);
                collection.add(obj);
            }

            return sortingType.sort(collection, field);
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

            V obj = StorageGson.getGson().fromJson(document.toJson(StorageGson.getJsonWriterSettings()), type);
            return obj;
        });
    }

    @Override
    public CompletableFuture<V> getFirst(String field, Object value, FilterType filterType) {
        return CompletableFuture.supplyAsync(() -> {
            Bson filter = getDocument(filterType, field, value);
            Document document = getCollection().find(filter).first();

            if (document == null) {
                return null;
            }

            V obj = StorageGson.getGson().fromJson(document.toJson(StorageGson.getJsonWriterSettings()), type);
            K key = (K) IdUtils.getId(type, obj);
            return obj;
        });
    }

    @Override
    public CompletableFuture<Void> save(V value) {
        return CompletableFuture.runAsync(() -> {
            K key = (K) IdUtils.getId(type, value);
            getCollection().replaceOne(
                    new Document(idFieldName, convertUUIDtoString(key)),
                    Document.parse(StorageGson.getGson().toJson(value)),
                    new ReplaceOptions().upsert(true)
            );
        });
    }

    @Override
    public CompletableFuture<Void> remove(V key) {
        return CompletableFuture.runAsync(() -> {
            try {
                K id = (K) IdUtils.getId(type, key);
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
                V obj = StorageGson.getGson().fromJson(document.toJson(StorageGson.getJsonWriterSettings()), type);
                collection.add(obj);
            }

            return collection;
        });
    }
}
