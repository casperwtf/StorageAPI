package wtf.casper.storageapi.impl.fstorage;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.zaxxer.hikari.HikariDataSource;
import lombok.SneakyThrows;
import lombok.extern.java.Log;
import wtf.casper.storageapi.Credentials;
import wtf.casper.storageapi.FieldStorage;
import wtf.casper.storageapi.FilterType;
import wtf.casper.storageapi.SortingType;
import wtf.casper.storageapi.cache.Cache;
import wtf.casper.storageapi.cache.CaffeineCache;
import wtf.casper.storageapi.id.exceptions.IdNotFoundException;
import wtf.casper.storageapi.id.utils.IdUtils;
import wtf.casper.storageapi.misc.ConstructableValue;
import wtf.casper.storageapi.misc.ISQLStorage;
import wtf.casper.storageapi.utils.Constants;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

@Log
public abstract class SQLFStorage<K, V> implements ConstructableValue<K, V>, FieldStorage<K, V>, ISQLStorage<K, V> {

    protected final Class<K> keyClass;
    protected final Class<V> valueClass;
    private final HikariDataSource ds;
    private final String table;
    private Cache<K, V> cache = new CaffeineCache<>(Caffeine.newBuilder()
            .expireAfterWrite(10, TimeUnit.MINUTES)
            .build());

    public SQLFStorage(final Class<K> keyClass, final Class<V> valueClass, final String table, final Credentials credentials) {
        this(keyClass, valueClass, table, credentials.getHost(), credentials.getPort(), credentials.getDatabase(), credentials.getUsername(), credentials.getPassword());
    }

    public SQLFStorage(final Class<K> keyClass, final Class<V> valueClass, final Credentials credentials) {
        this(keyClass, valueClass, credentials.getTable(), credentials.getHost(), credentials.getPort(), credentials.getDatabase(), credentials.getUsername(), credentials.getPassword());
    }


    @SneakyThrows
    public SQLFStorage(final Class<K> keyClass, final Class<V> valueClass, final String table, final String host, final int port, final String database, final String username, final String password) {
        if (true) {
            throw new RuntimeException(this.getClass().getSimpleName() + " is not implemented yet!");
        }
        this.keyClass = keyClass;
        this.valueClass = valueClass;
        this.table = table;
        this.ds = new HikariDataSource();
        this.ds.setMaximumPoolSize(20);
        this.ds.setDriverClassName("org.mariadb.jdbc.Driver");
        this.ds.setJdbcUrl("jdbc:mysql://" + host + ":" + port + "/" + database + "?allowPublicKeyRetrieval=true&autoReconnect=true&useSSL=false");
        this.ds.addDataSourceProperty("user", username);
        this.ds.addDataSourceProperty("password", password);
        this.ds.setAutoCommit(true);
    }

    @Override
    public HikariDataSource getDataSource() {
        return ds;
    }

    @Override
    public Logger logger() {
        return log;
    }

    @Override
    public String getTable() {
        return table;
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
    public Cache<K, V> cache() {
        return this.cache;
    }

    @Override
    public void cache(Cache<K, V> cache) {
        this.cache = cache;
    }

    @Override
    public CompletableFuture<Void> deleteAll() {
        return CompletableFuture.runAsync(() -> {
            execute("DELETE FROM " + this.table);
        }, Constants.EXECUTOR);
    }

    @SneakyThrows
    public CompletableFuture<Collection<V>> get(final String field, Object value, FilterType filterType, SortingType sortingType) {
        throw new RuntimeException(this.getClass().getSimpleName() + " is not implemented yet!");
    }

    @Override
    public CompletableFuture<V> get(K key) {
        throw new RuntimeException(this.getClass().getSimpleName() + " is not implemented yet!");
    }

    @Override
    public CompletableFuture<V> getFirst(String field, Object value, FilterType filterType) {
        throw new RuntimeException(this.getClass().getSimpleName() + " is not implemented yet!");
    }

    @Override
    public CompletableFuture<Void> save(final V value) {
        throw new RuntimeException(this.getClass().getSimpleName() + " is not implemented yet!");
    }

    @Override
    public CompletableFuture<Void> remove(final V value) {
        throw new RuntimeException(this.getClass().getSimpleName() + " is not implemented yet!");
    }

    @Override
    @SneakyThrows
    public CompletableFuture<Void> write() {
        return CompletableFuture.runAsync(() -> {
            this.saveAll(this.cache.asMap().values());
        }, Constants.EXECUTOR);
    }

    @Override
    public CompletableFuture<Void> close() {
        return CompletableFuture.runAsync(() -> {
            try {
                this.ds.getConnection().close();
            } catch (final SQLException e) {
                e.printStackTrace();
            }
        }, Constants.EXECUTOR);
    }

    @Override
    public CompletableFuture<Collection<V>> allValues() {
        throw new RuntimeException(this.getClass().getSimpleName() + " is not implemented yet!");
    }
}
