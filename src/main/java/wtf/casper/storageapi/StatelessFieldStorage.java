package wtf.casper.storageapi;

import wtf.casper.storageapi.misc.ConstructableValue;
import wtf.casper.storageapi.misc.KeyValue;
import wtf.casper.storageapi.utils.ReflectionUtil;
import wtf.casper.storageapi.utils.StorageAPIConstants;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

public interface StatelessFieldStorage<K, V> {

    /**
     * @param field the field to search for.
     * @param value the value to search for.
     * @return a future that will complete with a collection of all values that match the given field and value.
     */
    default CompletableFuture<Collection<V>> get(final String field, final Object value) {
        return get(field, value, FilterType.EQUALS, SortingType.NONE);
    }

    /**
     * @param field       the field to search for.
     * @param value       the value to search for.
     * @param filterType  the filter type to use.
     * @param sortingType the sorting type to use.
     * @return a future that will complete with a collection of all values that match the given field and value.
     */
    default CompletableFuture<Collection<V>> get(final String field, final Object value, final FilterType filterType, final SortingType sortingType) {
        return get(Filter.of(field, value, filterType, sortingType));
    };

    /**
     * @param filters the filters to use.
     * @return a future that will complete with a collection of all value that match the given filters.
     */
    default CompletableFuture<Collection<V>> get(Filter... filters) {
        return get(-1, filters);
    };

    /**
     * @param limit   the limit of values to return.
     * @param filters the filters to use.
     * @return a future that will complete with a collection of all value that match the given filters.
     */
    default CompletableFuture<Collection<V>> get(int limit, Filter... filters) {
        return get(0, limit, filters);
    }

    CompletableFuture<Collection<V>> get(int skip, int limit, Filter... filters);

    /**
     * @param key the key to search for.
     * @return a future that will complete with the value that matches the given key.
     * The value may be null if the key is not found.
     */
    CompletableFuture<V> get(final K key);

    /**
     * @param key the key to search for.
     * @return a future that will complete with the value that matches the given key or a generated value if not found.
     */
    default CompletableFuture<V> getOrDefault(final K key) {
        return get(key).thenApply((v) -> {

            if (v != null) {
                return v;
            }

            if (this instanceof ConstructableValue<?, ?>) {
                v = ((ConstructableValue<K, V>) this).constructValue(key);
                if (v == null) {
                    throw new RuntimeException("Failed to create default value for V with key " + key + ". " +
                            "Please create a constructor in V for only the key");
                }
                return v;
            }

            if (this instanceof KeyValue<?, ?>) {
                KeyValue<K, V> keyValueGetter = (KeyValue<K, V>) this;
                try {
                    return ReflectionUtil.createInstance(keyValueGetter.value(), key);
                } catch (final Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException("Failed to create default value for V with key " + key + ". " +
                            "Please create a constructor in V for only the key.", e);
                }
            }

            try {
                if (getClass().getGenericSuperclass() instanceof ParameterizedType parameterizedType) {
                    Type type = parameterizedType.getActualTypeArguments()[1];
                    Class<V> aClass = (Class<V>) Class.forName(type.getTypeName());
                    return ReflectionUtil.createInstance(aClass, key);
                }

                throw new RuntimeException("Failed to create default value for V with key " + key + ". " +
                        "Please create a constructor in V for only the key.");

            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("Failed to create default value for V with key " + key + ". " +
                        "Please create a constructor in V for only the key.");
            }
        });
    }

    /**
     * @param field the field to search for.
     * @param value the value to search for.
     * @return a future that will complete with the first value that matches the given field and value.
     */
    default CompletableFuture<V> getFirst(final String field, final Object value) {
        return getFirst(field, value, FilterType.EQUALS);
    }

    /**
     * @param field      the field to search for.
     * @param value      the value to search for.
     * @param filterType the filter type to use.
     * @return a future that will complete with the first value that matches the given field and value.
     */
    default CompletableFuture<V> getFirst(final String field, final Object value, FilterType filterType) {
        return get(1, Filter.of(field, value, filterType, SortingType.NONE)).thenApply((values) -> {
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
    default CompletableFuture<Boolean> migrate(final StatelessFieldStorage<K, V> storage) {
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
