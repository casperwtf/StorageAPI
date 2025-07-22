package wtf.casper.storageapi;

import wtf.casper.storageapi.misc.ConstructableValue;
import wtf.casper.storageapi.misc.KeyValue;
import wtf.casper.storageapi.utils.ReflectionUtil;
import wtf.casper.storageapi.utils.StorageAPIConstants;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface KeyedStorage<K, V> {

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
                    throw new RuntimeException("Failed to create default value for V with key " + key
                            + ". Please create a constructor in V for only the key.");
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
     * @param key the key to check for.
     * @return a future that will complete with a boolean that represents whether the storage contains a value that matches the given field and value.
     */
    default CompletableFuture<Boolean> contains(K key) {
        return CompletableFuture.supplyAsync(() -> get(key).join() != null, StorageAPIConstants.DB_THREAD_POOL);
    }

    /**
     * @param storage the storage to migrate from. The data will be copied from the given storage to this storage.
     * @return a future that will complete with a boolean that represents whether the migration was successful.
     */
    default CompletableFuture<Boolean> migrate(final KeyedStorage<K, V> storage) {
        return CompletableFuture.supplyAsync(() -> {
            Collection<V> vs = storage.allValues().join();
            saveAll(vs).join(); // save all will batch if the implementation supports it (mongo for example)
            return true;
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    CompletableFuture<Void> renameField(String path, String newPath);

    CompletableFuture<Void> renameFields(Map<String, String> pathToNewPath);

    /**
     * @return a future that will complete with a collection of all values in the storage.
     */
    CompletableFuture<Collection<V>> allValues();
}
