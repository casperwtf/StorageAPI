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
import wtf.casper.storageapi.id.utils.IdUtils;
import wtf.casper.storageapi.misc.ConstructableValue;
import wtf.casper.storageapi.misc.ISQLStorage;
import wtf.casper.storageapi.utils.Constants;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

// https://mariadb.com/kb/en/json-functions/
// https://mariadb.com/kb/en/json-data-type/
@Log
public abstract class MariaDBFStorage<K, V> implements ConstructableValue<K, V>, FieldStorage<K, V>, ISQLStorage<K, V> {

    protected final Class<K> keyClass;
    protected final Class<V> valueClass;
    private final HikariDataSource ds;
    private final String table;
    private Cache<K, V> cache = new CaffeineCache<>(Caffeine.newBuilder()
            .expireAfterWrite(10, TimeUnit.MINUTES)
            .build());

    public MariaDBFStorage(final Class<K> keyClass, final Class<V> valueClass, final String table, final Credentials credentials) {
        this(keyClass, valueClass, table, credentials.getHost(), credentials.getPort(), credentials.getDatabase(), credentials.getUsername(), credentials.getPassword());
    }

    public MariaDBFStorage(final Class<K> keyClass, final Class<V> valueClass, final Credentials credentials) {
        this(keyClass, valueClass, credentials.getTable(), credentials.getHost(), credentials.getPort(), credentials.getDatabase(), credentials.getUsername(), credentials.getPassword());
    }

    @SneakyThrows
    public MariaDBFStorage(final Class<K> keyClass, final Class<V> valueClass, final String table, final String host, final int port, final String database, final String username, final String password) {
        if (true) {
            throw new RuntimeException(this.getClass().getSimpleName() + " is not implemented yet!");
        }
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
        return CompletableFuture.supplyAsync(() -> {
            Collection<V> values;

            if (!filterType.isApplicable(value.getClass())) {
                return new ArrayList<>();
            }

            switch (filterType) {
                case EQUALS -> values = _equals(field, value);
                case GREATER_THAN -> values = _gt(field, value);
                case GREATER_THAN_OR_EQUAL_TO -> values = _gte(field, value);
                case LESS_THAN_OR_EQUAL_TO -> values = _lte(field, value);
                case LESS_THAN -> values = _lt(field, value);
                case CONTAINS -> values = _contains(field, value);
                default -> throw new IllegalArgumentException("FilterType " + filterType + " is not implemented yet!");
            }

            return sortingType.sort(values, field);
        }, Constants.EXECUTOR);
    }

    @Override
    public CompletableFuture<V> get(K key) {
        return CompletableFuture.supplyAsync(() -> {
            if (cache.contains(key)) {
                return cache.getIfPresent(key);
            }

            this.query("SELECT * FROM " + this.getTable() + " WHERE id = ?", ps -> {
                ps.setString(1, key.toString());
            }, rs -> {
                if (rs.next()) {
                    final String json = rs.getString("json");
                    final V value = Constants.getGson().fromJson(json, this.value());
                    this.cache.put(key, value);
                }
            });
            return this.cache.getIfPresent(key);
        }, Constants.EXECUTOR);
    }

    @Override
    public CompletableFuture<V> getFirst(String field, Object value, FilterType filterType) {
        throw new RuntimeException(this.getClass().getSimpleName() + " is not implemented yet!");
    }

    @Override
    public CompletableFuture<Void> save(final V value) {
        return CompletableFuture.runAsync(() -> {
            final String json = Constants.getGson().toJson(value);
            this.execute("INSERT INTO " + this.getTable() + " (id, json) VALUES (?, ?) ON DUPLICATE KEY UPDATE json = ?", ps -> {
                ps.setString(1, IdUtils.getId(this.value(), value).toString());
                ps.setString(2, json);
                ps.setString(3, json);
            });
        }, Constants.EXECUTOR);
    }

    @Override
    public CompletableFuture<Void> remove(final V value) {
        return CompletableFuture.runAsync(() -> {
            this.execute("DELETE FROM " + this.getTable() + " WHERE id = ?", ps -> {
                ps.setString(1, IdUtils.getId(this.value(), value).toString());
            });
        }, Constants.EXECUTOR);
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

    private Collection<V> _equals(final String field, Object value) {
        List<V> values = new ArrayList<>();

        this.query("SELECT * FROM " + table + " WHERE JSON_EXTRACT(json '$.?') = ?", ps -> {
            ps.setString(1, field);
            ps.setString(2, value.toString());
        }, rs -> {
            while (rs.next()) {
                final String json = rs.getString("json");
                final V obj = Constants.getGson().fromJson(json, this.value());
                values.add(obj);
            }
        }).join();

        return values;
    }

    private Collection<V> _gt(String field, Object value) {
        List<V> values = new ArrayList<>();

        this.query("SELECT * FROM " + table + " WHERE JSON_EXTRACT(json '$.?') > ?;", ps -> {
            ps.setString(1, field);
            ps.setString(2, value.toString());
        }, rs -> {
            while (rs.next()) {
                final String json = rs.getString("json");
                final V obj = Constants.getGson().fromJson(json, this.value());
                values.add(obj);
            }
        }).join();

        return values;
    }

    private Collection<V> _gte(String field, Object value) {
        List<V> values = new ArrayList<>();

        this.query("SELECT * FROM " + table + " WHERE JSON_EXTRACT(json '$.?') >= ?;", ps -> {
            ps.setString(1, field);
            ps.setString(2, value.toString());
        }, rs -> {
            while (rs.next()) {
                final String json = rs.getString("json");
                final V obj = Constants.getGson().fromJson(json, this.value());
                values.add(obj);
            }
        }).join();

        return values;
    }

    private Collection<V> _lte(String field, Object value) {
        List<V> values = new ArrayList<>();

        this.query("SELECT * FROM " + table + " WHERE JSON_EXTRACT(json '$.?') <= ?;", ps -> {
            ps.setString(1, field);
            ps.setString(2, value.toString());
        }, rs -> {
            while (rs.next()) {
                final String json = rs.getString("json");
                final V obj = Constants.getGson().fromJson(json, this.value());
                values.add(obj);
            }
        }).join();

        return values;
    }

    private Collection<V> _lt(String field, Object value) {
        List<V> values = new ArrayList<>();

        this.query("SELECT * FROM " + table + " WHERE JSON_EXTRACT(json '$.?') < ?;", ps -> {
            ps.setString(1, field);
            ps.setString(2, value.toString());
        }, rs -> {
            while (rs.next()) {
                final String json = rs.getString("json");
                final V obj = Constants.getGson().fromJson(json, this.value());
                values.add(obj);
            }
        }).join();

        return values;
    }

    private Collection<V> _contains(String field, Object value) {
        List<V> values = new ArrayList<>();

        this.query("SELECT * FROM " + table + " WHERE JSON_EXTRACT(json '$.?') LIKE '%?%'", ps -> {
            ps.setString(1, field);
            ps.setString(2, value.toString());
        }, rs -> {
            while (rs.next()) {
                final String json = rs.getString("json");
                final V obj = Constants.getGson().fromJson(json, this.value());
                values.add(obj);
            }
        }).join();

        return values;
    }

    private void createTable() {
        execute("CREATE TABLE IF NOT EXISTS " + this.table + " (id VARCHAR(255) PRIMARY KEY, json LONGTEXT)");
    }
}
