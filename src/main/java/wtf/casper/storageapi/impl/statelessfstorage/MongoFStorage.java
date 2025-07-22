package wtf.casper.storageapi.impl.statelessfstorage;


import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import lombok.Getter;
import lombok.extern.java.Log;
import org.bson.Document;
import wtf.casper.storageapi.*;
import wtf.casper.storageapi.id.utils.IdUtils;
import wtf.casper.storageapi.misc.ConstructableValue;
import wtf.casper.storageapi.misc.MongoStorage;
import wtf.casper.storageapi.misc.MongoProvider;
import wtf.casper.storageapi.utils.StorageAPIConstants;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Log
public abstract class MongoFStorage<K, V> implements FieldStorage<K, V>, ConstructableValue<K, V>, MongoStorage {

    protected final Class<K> keyClass;
    protected final Class<V> valueClass;
    protected final String idFieldName;
    protected final MongoClient mongoClient;
    protected final String uri;
    @Getter
    protected final MongoCollection<Document> collection;
    protected final ReplaceOptions replaceOptions = new ReplaceOptions().upsert(true);

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
        this.uri = uri;

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
    public CompletableFuture<Collection<V>> get(int skip, int limit, Condition... conditions) {
        return CompletableFuture.supplyAsync(() -> {
            if (conditions.length == 0) {
                return allValues().join();
            }

            boolean hasLimit = limit > 0;
            List<List<Condition>> group = Condition.group(conditions);
            boolean hasOr = group.size() > 1;

            Document query = new Document();
            List<Document> orConditions = new ArrayList<>();

            for (List<Condition> conditionGroup : group) {
                Document andQuery = andFilters(conditionGroup.toArray(new Condition[0]));

                if (hasOr) {
                    orConditions.add(andQuery);
                } else {
                    query = andQuery;
                }
            }

            if (hasOr) {
                query.append("$or", orConditions);
            }

            FindIterable<Document> iterable = collection.find(query);
            if (hasLimit) {
                iterable.limit(limit);
            }

            if (skip > 0) {
                iterable.skip(skip);
            }

            Condition sortCondition = conditions[0];
            if (sortCondition != null && sortCondition.sortingType() == SortingType.ASCENDING) {
                iterable.sort(new Document(sortCondition.key(), 1));
            } else if (sortCondition != null && sortCondition.sortingType() == SortingType.DESCENDING) {
                iterable.sort(new Document(sortCondition.key(), -1));
            }

            List<V> values = new ArrayList<>();
            for (Document document : iterable) {
                values.add(StorageAPIConstants.getGson().fromJson(document.toJson(), valueClass));
            }
            return values;
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<V> get(K key) {
        return CompletableFuture.supplyAsync(() -> {
            Document document = collection.find(new Document(idFieldName, IdUtils.getId(valueClass, key))).first();
            if (document == null) {
                return null;
            }
            return StorageAPIConstants.getGson().fromJson(document.toJson(), valueClass);
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Void> save(V value) {
        return CompletableFuture.supplyAsync(() -> {
            Document document = Document.parse(StorageAPIConstants.getGson().toJson(value));
            collection.replaceOne(new Document(idFieldName, IdUtils.getId(valueClass, value)), document, replaceOptions);
            return null;
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Void> saveAll(Collection<V> values) {
        return CompletableFuture.supplyAsync(() -> {
            for (V value : values) {
                Document document = Document.parse(StorageAPIConstants.getGson().toJson(value));
                collection.replaceOne(new Document(idFieldName, IdUtils.getId(valueClass, value)), document, replaceOptions);
            }
            return null;
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Void> remove(V key) {
        return CompletableFuture.supplyAsync(() -> {
            collection.deleteOne(new Document(idFieldName, IdUtils.getId(valueClass, key)));
            return null;
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Void> write() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> deleteAll() {
        return CompletableFuture.supplyAsync(() -> {
            collection.deleteMany(new Document());
            return null;
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Boolean> contains(String field, Object value) {
        return CompletableFuture.supplyAsync(() -> collection.find(new Document(field, value)).first() != null, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Collection<V>> allValues() {
        return CompletableFuture.supplyAsync(() -> {
            List<V> values = new ArrayList<>();
            FindIterable<Document> iterable = collection.find();
            for (Document document : iterable) {
                values.add(StorageAPIConstants.getGson().fromJson(document.toJson(), valueClass));
            }
            return values;
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Void> addIndex(String field) {
        return CompletableFuture.supplyAsync(() -> {
            collection.createIndex(new Document(field, 1));
            return null;
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Void> removeIndex(String field) {
        return CompletableFuture.supplyAsync(() -> {
            collection.dropIndex(field);
            return null;
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    private Document andFilters(Condition... conditions) {
        Document query = new Document();
        for (Condition condition : conditions) {
            query.putAll(filterToDocument(condition));
        }
        return query;
    }

    private Document filterToDocument(Condition condition) {
        switch (condition.conditionType()) {
            case STARTS_WITH -> {
                return new Document(condition.key(), new Document("$regex", "^" + condition.value()).append("$options", "i"));
            }
            case LESS_THAN, NOT_GREATER_THAN_OR_EQUAL_TO -> {
                return new Document(condition.key(), new Document("$lt", condition.value()));
            }
            case EQUALS -> {
                return new Document(condition.key(), condition.value());
            }
            case GREATER_THAN, NOT_LESS_THAN_OR_EQUAL_TO -> {
                return new Document(condition.key(), new Document("$gt", condition.value()));
            }
            case CONTAINS -> {
                return new Document(condition.key(), new Document("$regex", condition.value()).append("$options", "i"));
            }
            case ENDS_WITH -> {
                return new Document(condition.key(), new Document("$regex", condition.value() + "$").append("$options", "i"));
            }
            case LESS_THAN_OR_EQUAL_TO, NOT_GREATER_THAN -> {
                return new Document(condition.key(), new Document("$lte", condition.value()));
            }
            case GREATER_THAN_OR_EQUAL_TO, NOT_LESS_THAN -> {
                return new Document(condition.key(), new Document("$gte", condition.value()));
            }
            case NOT_EQUALS -> {
                return new Document(condition.key(), new Document("$ne", condition.value()));
            }
            case NOT_CONTAINS -> {
                return new Document(condition.key(), new Document("$not", new Document("$regex", condition.value()).append("$options", "i")));
            }
            case NOT_STARTS_WITH -> {
                return new Document(condition.key(), new Document("$not", new Document("$regex", "^" + condition.value()).append("$options", "i")));
            }
            case NOT_ENDS_WITH -> {
                return new Document(condition.key(), new Document("$not", new Document("$regex", condition.value() + "$").append("$options", "i")));
            }
            default -> {
                throw new IllegalArgumentException("Unknown filter type: " + condition.conditionType());
            }
        }
    }
}
