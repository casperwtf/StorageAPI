package wtf.casper.storageapi.impl.kvstorage;

import com.zaxxer.hikari.HikariDataSource;
import lombok.SneakyThrows;
import lombok.extern.java.Log;
import wtf.casper.storageapi.Credentials;
import wtf.casper.storageapi.misc.SQLStorage;
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
public class SqlKVStorage<K, V> implements SQLStorage<K, V> {

    private final HikariDataSource ds;
    private final Class<K> keyClass;
    private final Class<V> valueClass;
    private final String table;

    public SqlKVStorage(final Class<K> keyClass, final Class<V> valueClass, final String table, final Credentials credentials) {
        this(keyClass, valueClass, table, credentials.getHost(), credentials.getPort(3306), credentials.getDatabase(), credentials.getUsername(), credentials.getPassword());
    }

    @SneakyThrows
    public SqlKVStorage(final Class<K> keyClass, final Class<V> valueClass, final String table, final String host, final int port, final String database, final String username, final String password) {
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
            execute("ALTER TABLE " + this.table + " CHANGE " + path + " " + newPath + " TEXT;");
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Void> renameFields(Map<String, String> pathToNewPath) {
        return CompletableFuture.runAsync(() -> {
            for (Map.Entry<String, String> entry : pathToNewPath.entrySet()) {
                execute("ALTER TABLE " + this.table + " CHANGE " + entry.getKey() + " " + entry.getValue() + " TEXT;");
            }
        }, StorageAPIConstants.DB_THREAD_POOL);
    }
}