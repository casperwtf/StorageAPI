package wtf.casper.storageapi;

import wtf.casper.storageapi.utils.StorageAPIConstants;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface FieldStorage<K, V> {

    /**
     * @param query The query to execute
     * @return a future that will complete with a collection of all values that match the given field and value.
     */
    CompletableFuture<Collection<V>> get(Query query);

    /**
     * @param query The query to remove
     */
    CompletableFuture<Void> remove(final Query query);

    /**
     * Executes an aggregation query on the storage and returns the result as a CompletableFuture.
     *
     * @param query The query containing aggregation details, such as the fields, functions, and filters to apply.
     * @return A CompletableFuture that completes with the result of the aggregation query as an AggregationResult object.
     */
    CompletableFuture<List<AggregationResult>> aggregate(final Query query);

    /**
     * @param value the value to save.
     */
    CompletableFuture<Void> save(final V value);

    /**
     * @param values the values to save.
     */
    default CompletableFuture<Void> saveAll(final Collection<V> values) {
        // designed to be naive approach that is overridden for batched impls
        return CompletableFuture.runAsync(() -> values.forEach(v -> save(v).join()), StorageAPIConstants.DB_THREAD_POOL);
    }

    /**
     * Writes the storage to disk.
     */
    CompletableFuture<Void> write();

    /**
     * Deletes the storage from disk.
     */
    CompletableFuture<Void> deleteAll();

    /**
     * Closes the storage/storage connection.
     */
    default CompletableFuture<Void> close() {
        return CompletableFuture.completedFuture(null);
    }

    /**
     * @param storage the storage to migrate from. The data will be copied from the given storage to this storage.
     * @return a future that will complete with a boolean that represents whether the migration was successful.
     */
    default CompletableFuture<Boolean> migrate(final FieldStorage<K, V> storage) {
        return CompletableFuture.supplyAsync(() -> {
            storage.allValues().thenAccept(this::saveAll).join();
            return true;
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    /**
     * @return a future that will complete with a collection of all values in the storage.
     */
    CompletableFuture<Collection<V>> allValues();

    /**
     * Adds an index to the storage.
     * @param field the field to add an index for.
     * @return a future that will complete when the index has been added.
     */
    CompletableFuture<Void> index(String field);

    /**
     * Removes an index from the storage.
     * @param field the field to remove the index for.
     * @return a future that will complete when the index has been removed.
     */
    CompletableFuture<Void> unindex(String field);
}
