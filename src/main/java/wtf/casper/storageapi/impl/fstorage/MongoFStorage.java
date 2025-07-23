package wtf.casper.storageapi.impl.fstorage;


import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.DeleteOptions;
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
public class MongoFStorage<K, V> implements FieldStorage<K, V>, ConstructableValue<K, V>, MongoStorage {

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
    public CompletableFuture<Collection<V>> get() {
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
    public CompletableFuture<Collection<V>> get(Query query) {
        return CompletableFuture.supplyAsync(() -> {
            boolean hasLimit = query.limit() > 0;
            List<List<Condition>> group = Condition.group(query.conditions().toArray(new Condition[0]));
            boolean hasOr = group.size() > 1;

            Document queryDocument = new Document();
            List<Document> orConditions = new ArrayList<>();

            for (List<Condition> conditionGroup : group) {
                Document andQuery = andFilters(conditionGroup.toArray(new Condition[0]));

                if (hasOr) {
                    orConditions.add(andQuery);
                } else {
                    queryDocument = andQuery;
                }
            }

            if (hasOr) {
                queryDocument.append("$or", orConditions);
            }

            FindIterable<Document> iterable = collection.find(queryDocument);
            if (hasLimit) {
                iterable.limit(query.limit());
            }

            if (query.offset() > 0) {
                iterable.skip(query.offset());
            }


            Sort sort = query.sorts().isEmpty() ? null : query.sorts().get(0);
            if (sort != null && sort.sortingType() == SortingType.ASCENDING) {
                iterable.sort(new Document(sort.field(), 1));
            } else if (sort != null && sort.sortingType() == SortingType.DESCENDING) {
                iterable.sort(new Document(sort.field(), -1));
            }

            List<V> values = new ArrayList<>();
            for (Document document : iterable) {
                values.add(StorageAPIConstants.getGson().fromJson(document.toJson(), valueClass));
            }
            return values;
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Void> remove(Query query) {
        return CompletableFuture.runAsync(() -> {
            boolean hasLimit = query.limit() > 0;
            List<List<Condition>> group = Condition.group(query.conditions().toArray(new Condition[0]));
            boolean hasOr = group.size() > 1;

            Document queryDocument = new Document();
            List<Document> orConditions = new ArrayList<>();

            for (List<Condition> conditionGroup : group) {
                Document andQuery = andFilters(conditionGroup.toArray(new Condition[0]));

                if (hasOr) {
                    orConditions.add(andQuery);
                } else {
                    queryDocument = andQuery;
                }
            }

            if (hasOr) {
                queryDocument.append("$or", orConditions);
            }

            boolean hasOffset = query.offset() > 0;
            if (hasLimit || hasOffset) {
                List<Document> documents = new ArrayList<>();
                collection.find(queryDocument)
                        .limit(query.limit() == -1 ? 0 : query.limit() + query.offset())
                        .skip(query.offset())
                        .into(documents);
                collection.deleteMany(new Document("$or", documents));
                return;
            }

            collection.deleteMany(queryDocument);
        }, StorageAPIConstants.DB_THREAD_POOL);

    }

    @Override
    public CompletableFuture<List<AggregationResult>> aggregate(Query query) {
        return CompletableFuture.supplyAsync(() -> {
            List<AggregationResult> results = new ArrayList<>();
            Document queryDocument = andFilters(query.conditions().toArray(new Condition[0]));
            Document aggregationDocument = new Document();
            for (Aggregation aggregation : query.aggregations()) {
                aggregationDocument.append(aggregation.field(), aggregation(aggregation));
            }

            for (Document document : collection.aggregate(List.of(queryDocument, aggregationDocument))) {
                results.add(new AggregationResult(document.getString(idFieldName), document.getDouble(aggregationDocument.keySet().iterator().next())));
            }

            return results;
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
    public CompletableFuture<Void> index(String field) {
        return CompletableFuture.supplyAsync(() -> {
            collection.createIndex(new Document(field, 1));
            return null;
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Void> unindex(String field) {
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

    private Document aggregation(Aggregation aggregation) {
        switch (aggregation.function()) {
            case AVG -> {
                return new Document("$avg", "$" + aggregation.field());
            }
            case COUNT -> {
                return new Document("$count", aggregation.field());
            }
            case MAX -> {
                return new Document("$max", "$" + aggregation.field());
            }
            case MIN -> {
                return new Document("$min", "$" + aggregation.field());
            }
            case SUM -> {
                return new Document("$sum", "$" + aggregation.field());
            }
            default -> {
                throw new IllegalArgumentException("Unknown aggregation function: " + aggregation.function());
            }
        }
    }
}
