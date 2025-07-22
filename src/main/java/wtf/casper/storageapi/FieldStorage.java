package wtf.casper.storageapi;

import wtf.casper.storageapi.utils.StorageAPIConstants;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

public interface FieldStorage<K, V> {

    /**
     * @param field the field to search for.
     * @param value the value to search for.
     * @return a future that will complete with a collection of all values that match the given field and value.
     */
    default CompletableFuture<Collection<V>> get(final String field, final Object value) {
        return get(field, value, ConditionType.EQUALS, SortingType.NONE);
    }

    /**
     * @param field       the field to search for.
     * @param value       the value to search for.
     * @param conditionType  the filter type to use.
     * @param sortingType the sorting type to use.
     * @return a future that will complete with a collection of all values that match the given field and value.
     */
    default CompletableFuture<Collection<V>> get(final String field, final Object value, final ConditionType conditionType, final SortingType sortingType) {
        return get(Condition.of(field, value, conditionType, sortingType));
    }

    /**
     * @param conditions the filters to use.
     * @return a future that will complete with a collection of all value that match the given filters.
     */
    default CompletableFuture<Collection<V>> get(Condition... conditions) {
        return get(-1, conditions);
    };

    /**
     * @param limit   the limit of values to return.
     * @param conditions the filters to use.
     * @return a future that will complete with a collection of all value that match the given filters.
     */
    default CompletableFuture<Collection<V>> get(int limit, Condition... conditions) {
        return get(0, limit, conditions);
    }

    CompletableFuture<Collection<V>> get(int skip, int limit, Condition... conditions);

    /**
     * @param key the key to search for.
     * @return a future that will complete with the value that matches the given key.
     * The value may be null if the key is not found.
     */
    CompletableFuture<V> get(final K key);

    /**
     * @param field the field to search for.
     * @param value the value to search for.
     * @return a future that will complete with the first value that matches the given field and value.
     */
    default CompletableFuture<V> getFirst(final String field, final Object value) {
        return getFirst(field, value, ConditionType.EQUALS);
    }

    /**
     * @param field      the field to search for.
     * @param value      the value to search for.
     * @param conditionType the filter type to use.
     * @return a future that will complete with the first value that matches the given field and value.
     */
    default CompletableFuture<V> getFirst(final String field, final Object value, ConditionType conditionType) {
        return get(1, Condition.of(field, value, conditionType, SortingType.NONE)).thenApply((values) -> {
            if (values.isEmpty()) {
                return null;
            }

            return values.iterator().next();
        });
    };


    /**
     * @param value the value to save.
     */
    CompletableFuture<Void> save(final V value);

    /**
     * @param values the values to save.
     */
    default CompletableFuture<Void> saveAll(final Collection<V> values) {
        return CompletableFuture.runAsync(() -> values.forEach(v -> save(v).join()), StorageAPIConstants.DB_THREAD_POOL);
    }

    /**
     * @param key the key to remove.
     */
    CompletableFuture<Void> remove(final V key);

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
     * @param field the field to search for.
     * @param value the value to search for.
     * @return a future that will complete with a boolean that represents whether the storage contains a value that matches the given field and value.
     */
    CompletableFuture<Boolean> contains(final String field, final Object value);

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
    CompletableFuture<Void> addIndex(String field);

    /**
     * Removes an index from the storage.
     * @param field the field to remove the index for.
     * @return a future that will complete when the index has been removed.
     */
    CompletableFuture<Void> removeIndex(String field);
}
