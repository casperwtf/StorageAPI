package wtf.casper.storageapi.impl.statelessfstorage;

import com.zaxxer.hikari.HikariDataSource;
import lombok.SneakyThrows;
import lombok.extern.java.Log;
import wtf.casper.storageapi.Credentials;
import wtf.casper.storageapi.FilterType;
import wtf.casper.storageapi.SortingType;
import wtf.casper.storageapi.cache.Cache;
import wtf.casper.storageapi.id.exceptions.IdNotFoundException;
import wtf.casper.storageapi.id.utils.IdUtils;
import wtf.casper.storageapi.misc.ISQLStorage;
import wtf.casper.storageapi.utils.Constants;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

@Log
public class StatelessSQLFStorage<K, V> implements ISQLStorage<K, V> {

    private final HikariDataSource ds;
    private final Class<K> keyClass;
    private final Class<V> valueClass;
    private final String table;

    public StatelessSQLFStorage(final Class<K> keyClass, final Class<V> valueClass, final String table, final Credentials credentials) {
        this(keyClass, valueClass, table, credentials.getHost(), credentials.getPort(), credentials.getDatabase(), credentials.getUsername(), credentials.getPassword());
    }

    @SneakyThrows
    public StatelessSQLFStorage(final Class<K> keyClass, final Class<V> valueClass, final String table, final String host, final int port, final String database, final String username, final String password) {
        if (true) {
            throw new RuntimeException(this.getClass().getSimpleName() + " is not implemented yet!");
        }
        this.keyClass = keyClass;
        this.valueClass = valueClass;
        this.table = table;
        this.ds = new HikariDataSource();
        this.ds.setMaximumPoolSize(20);
        this.ds.setDriverClassName("com.mysql.cj.jdbc.Driver");
        this.ds.setJdbcUrl("jdbc:mysql://" + host + ":" + port + "/" + database + "?allowPublicKeyRetrieval=true&autoReconnect=true&useSSL=false");
        this.ds.addDataSourceProperty("user", username);
        this.ds.addDataSourceProperty("password", password);
        this.ds.setConnectionTimeout(300000);
        this.ds.setConnectionTimeout(120000);
        this.ds.setLeakDetectionThreshold(300000);
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
        throw new RuntimeException(this.getClass().getSimpleName() + " is not implemented yet!");
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