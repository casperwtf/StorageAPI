package wtf.casper.storageapi.impl.statelesskvstorage;

import com.zaxxer.hikari.HikariDataSource;
import lombok.SneakyThrows;
import lombok.extern.java.Log;
import wtf.casper.storageapi.Credentials;
import wtf.casper.storageapi.misc.ISQLKVStorage;
import wtf.casper.storageapi.utils.StorageAPIConstants;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

@Log
public class StatelessMariaDBKVStorage<K, V> implements ISQLKVStorage<K, V> {
    private final HikariDataSource ds;
    private final Class<K> keyClass;
    private final Class<V> valueClass;
    private final String table;

    public StatelessMariaDBKVStorage(final Class<K> keyClass, final Class<V> valueClass, final String table, final Credentials credentials) {
        this(keyClass, valueClass, table, credentials.getHost(), credentials.getPort(3306), credentials.getDatabase(), credentials.getUsername(), credentials.getPassword());
    }

    public StatelessMariaDBKVStorage(final Class<K> keyClass, final Class<V> valueClass, final Credentials credentials) {
        this(keyClass, valueClass, credentials.getTable(), credentials.getHost(), credentials.getPort(3306), credentials.getDatabase(), credentials.getUsername(), credentials.getPassword());
    }

    @SneakyThrows
    public StatelessMariaDBKVStorage(final Class<K> keyClass, final Class<V> valueClass, final String table, final String host, final int port, final String database, final String username, final String password) {
        this.keyClass = keyClass;
        this.valueClass = valueClass;
        this.table = table;
        this.ds = new HikariDataSource();
        this.ds.setMaximumPoolSize(20);
        this.ds.setDriverClassName("org.mariadb.jdbc.Driver");
        this.ds.setJdbcUrl("jdbc:mariadb://" + host + ":" + port + "/" + database + "?allowPublicKeyRetrieval=true&autoReconnect=true&useSSL=false");
        this.ds.addDataSourceProperty("user", username);
        this.ds.addDataSourceProperty("password", password);
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
    public Class<K> key() {
        return keyClass;
    }

    @Override
    public Class<V> value() {
        return valueClass;
    }

    @Override
    public CompletableFuture<Void> write() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> deleteAll() {
        return CompletableFuture.runAsync(() -> {
            execute("DELETE FROM " + this.table + ";");
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Void> close() {
        return CompletableFuture.runAsync(() -> {
            try {
                this.ds.getConnection().close();
            } catch (final SQLException e) {
                e.printStackTrace();
            }
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Collection<V>> allValues() {
        return CompletableFuture.supplyAsync(() -> {
            final List<V> values = new ArrayList<>();
            ResultSet set = query("SELECT * FROM " + this.table, statement -> {
            }).join();

            try {
                while (set.next()) {
                    values.add(StorageAPIConstants.getGson().fromJson(set.getString("data"), this.valueClass));
                }
                set.close();
            } catch (final SQLException e) {
                e.printStackTrace();
            }

            return values;
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Void> renameField(String path, String newPath) {
        return CompletableFuture.runAsync(() -> {
            execute("UPDATE " + this.table + " SET data = JSON_SET(data, '$." + newPath + "', JSON_EXTRACT(data, '$." + path + "'));");
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Void> renameFields(Map<String, String> pathToNewPath) {
        return CompletableFuture.runAsync(() -> {
            pathToNewPath.forEach((path, newPath) -> {
                this.execute("UPDATE " + this.table + " SET data = JSON_SET(data, '$." + newPath + "', JSON_EXTRACT(data, '$." + path + "'))," +
                        " data = JSON_REMOVE(data, '$." + path + "')", statement -> {
                });
            });
        }, StorageAPIConstants.DB_THREAD_POOL);
    }
}