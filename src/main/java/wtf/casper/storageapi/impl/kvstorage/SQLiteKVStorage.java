package wtf.casper.storageapi.impl.kvstorage;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.zaxxer.hikari.HikariDataSource;
import lombok.SneakyThrows;
import lombok.extern.java.Log;
import wtf.casper.storageapi.KVStorage;
import wtf.casper.storageapi.cache.Cache;
import wtf.casper.storageapi.cache.CaffeineCache;
import wtf.casper.storageapi.id.exceptions.IdNotFoundException;
import wtf.casper.storageapi.id.utils.IdUtils;
import wtf.casper.storageapi.misc.ISQLKVStorage;
import wtf.casper.storageapi.utils.StorageAPIConstants;

import java.io.File;
import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

@Log
public abstract class SQLiteKVStorage<K, V> implements ISQLKVStorage<K, V>, KVStorage<K, V> {

    protected final Class<K> keyClass;
    protected final Class<V> valueClass;
    private final HikariDataSource ds;
    private final String table;
    private Cache<K, V> cache = new CaffeineCache<>(Caffeine.newBuilder()
            .expireAfterWrite(10, TimeUnit.MINUTES)
            .build());

    @SneakyThrows
    public SQLiteKVStorage(final Class<K> keyClass, final Class<V> valueClass, final File file, String table) {
        this.keyClass = keyClass;
        this.valueClass = valueClass;
        this.table = table;
        this.ds = new HikariDataSource();
        this.ds.setMaximumPoolSize(20);
        this.ds.setDriverClassName("org.sqlite.JDBC");
        this.ds.setJdbcUrl("jdbc:sqlite:" + file.getAbsolutePath());
        this.ds.setConnectionTimeout(120000);
        this.ds.setLeakDetectionThreshold(300000);
        this.ds.setAutoCommit(true);
        createTable();
    }

    @SneakyThrows
    public SQLiteKVStorage(final Class<K> keyClass, final Class<V> valueClass, final String table, final String connection) {
        this.keyClass = keyClass;
        this.valueClass = valueClass;
        this.table = table;
        this.ds = new HikariDataSource();
        this.ds.setMaximumPoolSize(20);
        this.ds.setDriverClassName("org.sqlite.JDBC");
        this.ds.setJdbcUrl(connection);
        this.ds.setConnectionTimeout(120000);
        this.ds.setLeakDetectionThreshold(300000);
        this.ds.setAutoCommit(true);
        createTable();
    }

    @Override
    public HikariDataSource dataSource() {
        return ds;
    }

    @Override
    public Logger logger() {
        return log;
    }

    @Override
    public String table() {
        return table;
    }

    @Override
    public Cache<K, V> cache() {
        return this.cache;
    }

    @Override
    public void cache(Cache<K, V> cache) {
        this.cache = cache;
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
            execute("DELETE FROM " + this.table + ";");
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Void> remove(final V value) {
        return CompletableFuture.runAsync(() -> {
            Field idField;
            try {
                idField = IdUtils.getIdField(valueClass);
            } catch (IdNotFoundException e) {
                throw new RuntimeException(e);
            }
            this.cache.invalidate((K) IdUtils.getId(this.valueClass, value));
            String field = idField.getName();
            this.execute("DELETE FROM " + this.table + " WHERE " + field + " = '" + IdUtils.getId(this.valueClass, value) + "';");
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    @SneakyThrows
    public CompletableFuture<Void> write() {
        return CompletableFuture.runAsync(() -> {
            this.saveAll(this.cache.asMap().values());
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Void> save(V value) {
        return CompletableFuture.runAsync(() -> {
            Object id = IdUtils.getId(value);
            if (id == null) {
                logger().warning("Could not find id field for " + value().getSimpleName());
                return;
            }

            String idName = IdUtils.getIdName(value());
            String json = StorageAPIConstants.getGson().toJson(value);
            executeUpdate("INSERT OR REPLACE INTO " + table + " (" + idName + ", json) VALUES (?, ?);", statement -> {
                statement.setString(1, id.toString());
                statement.setString(2, json);
            });
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Void> close() {
        return CompletableFuture.runAsync(this.ds::close, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Collection<V>> allValues() {
        return CompletableFuture.supplyAsync(() -> {
            final List<V> values = new ArrayList<>();

            query("SELECT * FROM " + this.table + ";", preparedStatement -> {}, resultSet -> {
                try {
                    while (resultSet.next()) {
                        values.add(StorageAPIConstants.getGson().fromJson(resultSet.getString("json"), this.valueClass));
                    }
                } catch (final SQLException e) {
                    e.printStackTrace();
                }
            }).join();

            return values;
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public void createTable() {
        String idName = IdUtils.getIdName(value());
        boolean isUUID = UUID.class.isAssignableFrom(IdUtils.getIdClass(value()));
        String idType = isUUID ? "VARCHAR(36) NOT NULL" : "VARCHAR(255) NOT NULL";
        idType = idName + " " + idType + " PRIMARY KEY";

        execute("CREATE TABLE IF NOT EXISTS " + table() + " (" + idType + ", json TEXT NOT NULL);");
    }

    @Override
    public CompletableFuture<Void> renameField(String path, String newPath) {
        return CompletableFuture.runAsync(() -> {
            cache().invalidateAll();
            execute("UPDATE " + this.table + " SET json = JSON_SET(json, '$." + newPath + "', JSON_EXTRACT(json, '$." + path + "'))," +
                    " json = JSON_REMOVE(json, '$." + path + "')", statement -> {
            });
            cache().invalidateAll();
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Void> renameFields(Map<String, String> pathToNewPath) {
        return CompletableFuture.runAsync(() -> {
            cache().invalidateAll();
            pathToNewPath.forEach((path, newPath) -> {
                execute("UPDATE " + this.table + " SET json = JSON_SET(json, '$." + newPath + "', JSON_EXTRACT(json, '$." + path + "'))," +
                        " json = JSON_REMOVE(json, '$." + path + "')", statement -> {
                });
            });
            cache().invalidateAll();
        }, StorageAPIConstants.DB_THREAD_POOL);
    }
}
