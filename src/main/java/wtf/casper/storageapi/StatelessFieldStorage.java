package wtf.casper.storageapi;

import dev.dejvokep.boostedyaml.YamlDocument;
import dev.dejvokep.boostedyaml.block.implementation.Section;
import wtf.casper.storageapi.misc.ConstructableValue;
import wtf.casper.storageapi.misc.KeyValue;
import wtf.casper.storageapi.utils.ReflectionUtil;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

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
    CompletableFuture<Collection<V>> get(final String field, final Object value, final FilterType filterType, final SortingType sortingType);

    /**
     * @param filters the filters to use.
     * @return a future that will complete with a collection of all value that match the given filters.
     */
    default CompletableFuture<Collection<V>> get(Filter... filters) {
        return CompletableFuture.supplyAsync(() -> {
            Collection<V> values = new ArrayList<>();
            List<List<Filter>> group = Filter.group(filters);

            for (List<Filter> filterList : group) {
                values.addAll(filterGroup(filterList));
            }

            values.removeIf(v -> Collections.frequency(values, v) > 1);
            return values;
        });
    }

    default Collection<V> filterGroup(List<Filter> filters) {
        Collection<V> values = new ArrayList<>();
        if (filters == null || filters.isEmpty()) {
            return values;
        }
        get(filters.get(0).key(), filters.get(0).value(), filters.get(0).filterType(), filters.get(0).sortingType()).thenAccept(values::addAll).join();
        if (values.isEmpty()) {
            return values;
        }

        if (filters.size() == 1) {
            return values;
        }

        for (int i = 1; i < filters.size(); ) {
            final int index = i;
            values.removeIf((v) -> {
                String[] allFields = filters.get(index).key().split("\\.");
                if (allFields.length == 1) {
                    return !filters.get(index).filterType().passes(v, filters.get(index).key(), filters.get(index).value());
                }

                Iterator<String> iterator = Arrays.stream(allFields).iterator();
                Object object = v;
                while (iterator.hasNext()) {
                    String field = iterator.next();
                    Optional<Object> optional = ReflectionUtil.getFieldValue(object, field);
                    if (optional.isEmpty()) {
                        return true;
                    }
                    object = optional.get();
                    if (!iterator.hasNext()) {
                        return !filters.get(index).filterType().passes(object, field, filters.get(index).value());
                    }
                }

                return true;
            });

            i++;
        }
        return values;
    }

    /**
     * @param limit   the limit of values to return.
     * @param filters the filters to use.
     * @return a future that will complete with a collection of all value that match the given filters.
     */
    default CompletableFuture<Collection<V>> get(int limit, Filter... filters) {
        if (filters == null || filters.length == 0) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }

        if (limit <= 0) {
            return get(filters);
        }

        return CompletableFuture.supplyAsync(() -> {
            Collection<V> join = get(filters).join();
            if (join.size() <= limit) {
                return join;
            }

            return join.stream().limit(limit).toList();
        });
    }

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
                    throw new RuntimeException("Failed to create default value for " + v.getClass().getSimpleName() + " with key " + key
                            + ". Please create a constructor in " + v.getClass().getSimpleName() + " for only the key.");
                }
                return v;
            }

            if (this instanceof KeyValue<?, ?>) {
                KeyValue<K, V> keyValueGetter = (KeyValue<K, V>) this;
                try {
                    return ReflectionUtil.createInstance(keyValueGetter.value(), key);
                } catch (final Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException("Failed to create default value for " + v.getClass().getSimpleName() + " with key " + key + ". " +
                            "Please create a constructor in " + v.getClass().getSimpleName() + " for only the key.", e);
                }
            }

            try {
                if (getClass().getGenericSuperclass() instanceof ParameterizedType parameterizedType) {
                    Type type = parameterizedType.getActualTypeArguments()[1];
                    System.out.println(type.getTypeName());
                    Class<V> aClass = (Class<V>) Class.forName(type.getTypeName());
                    return ReflectionUtil.createInstance(aClass, key);
                }

                throw new RuntimeException("Failed to create default value for " + v.getClass().getSimpleName() + " with key " + key + ". " +
                        "Please create a constructor in " + v.getClass().getSimpleName() + " for only the key.");

            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("Failed to create default value for " + v.getClass().getSimpleName() + " with key " + key + ". " +
                        "Please create a constructor in " + v.getClass().getSimpleName() + " for only the key.");
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
    CompletableFuture<V> getFirst(final String field, final Object value, FilterType filterType);


    /**
     * @param value the value to save.
     */
    CompletableFuture<Void> save(final V value);

    /**
     * @param values the values to save.
     */
    default CompletableFuture<Void> saveAll(final Collection<V> values) {
        return CompletableFuture.runAsync(() -> {
            values.forEach(this::save);
        });
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
     * Closes the storage/storage connection.
     */
    default CompletableFuture<Void> close() {
        return CompletableFuture.runAsync(() -> {
        });
    }

    /**
     * @param field the field to search for.
     * @param value the value to search for.
     * @return a future that will complete with a boolean that represents whether the storage contains a value that matches the given field and value.
     */
    default CompletableFuture<Boolean> contains(final String field, final Object value) {
        return CompletableFuture.supplyAsync(() -> getFirst(field, value).join() != null);
    }

    /**
     * @param storage the storage to migrate from. The data will be copied from the given storage to this storage.
     * @return a future that will complete with a boolean that represents whether the migration was successful.
     */
    default CompletableFuture<Boolean> migrate(final StatelessFieldStorage<K, V> storage) {
        return CompletableFuture.supplyAsync(() -> {
            storage.allValues().thenAccept((values) -> {
                values.forEach(this::save);
            }).join();
            return true;
        });
    }

    /**
     * @param oldStorageSupplier supplier to provide the old storage
     * @param config             the config
     * @param path               the path to the storage
     * @return a future that will complete with a boolean that represents whether the migration was successful.
     */
    default CompletableFuture<Boolean> migrateFrom(Supplier<StatelessFieldStorage<K, V>> oldStorageSupplier, YamlDocument config, String path) {
        return CompletableFuture.supplyAsync(() -> {
            if (config == null) return false;
            Section section = config.getSection(path);
            if (section == null) return false;
            if (!section.getBoolean("migrate", false)) return false;
            section.set("migrate", false);
            try {
                config.save();
            } catch (IOException e) {
                e.printStackTrace();
            }
            // storage that we are migrating to the new storage
            StatelessFieldStorage<K, V> oldStorage = oldStorageSupplier.get();
            try {
                this.migrate(oldStorage).join();
                return true;
            } catch (Exception e) {
                return false;
            }
        });
    }

    /**
     * @return a future that will complete with a collection of all values in the storage.
     */
    CompletableFuture<Collection<V>> allValues();

    /**
     * @param field       the field to search for.
     * @param sortingType the sorting type to use.
     * @return a future that will complete with a collection of all values in the storage that match the given field and value.
     */
    default CompletableFuture<Collection<V>> allValues(String field, SortingType sortingType) {
        return CompletableFuture.supplyAsync(() -> {
            Collection<V> values = allValues().join();
            if (values.isEmpty()) {
                return values;
            }

            // Sort the values.
            return sortingType.sort(values, field);
        });
    }
}
