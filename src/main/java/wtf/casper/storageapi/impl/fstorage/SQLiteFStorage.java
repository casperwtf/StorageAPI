package wtf.casper.storageapi.impl.fstorage;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.zaxxer.hikari.HikariDataSource;
import lombok.SneakyThrows;
import lombok.extern.java.Log;
import wtf.casper.storageapi.FieldStorage;
import wtf.casper.storageapi.FilterType;
import wtf.casper.storageapi.SortingType;
import wtf.casper.storageapi.cache.Cache;
import wtf.casper.storageapi.cache.CaffeineCache;
import wtf.casper.storageapi.id.exceptions.IdNotFoundException;
import wtf.casper.storageapi.id.utils.IdUtils;
import wtf.casper.storageapi.misc.ISQLFStorage;
import wtf.casper.storageapi.utils.Constants;

import java.io.File;
import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

@Log
public abstract class SQLiteFStorage<K, V> implements ISQLFStorage<K, V>, FieldStorage<K, V> {

    protected final Class<K> keyClass;
    protected final Class<V> valueClass;
    private final HikariDataSource ds;
    private final String table;
    private Cache<K, V> cache = new CaffeineCache<>(Caffeine.newBuilder()
            .expireAfterWrite(10, TimeUnit.MINUTES)
            .build());

    @SneakyThrows
    public SQLiteFStorage(final Class<K> keyClass, final Class<V> valueClass, final File file, String table) {
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
    public SQLiteFStorage(final Class<K> keyClass, final Class<V> valueClass, final String table, final String connection) {
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
        });
    }

    @SneakyThrows
    public CompletableFuture<Collection<V>> get(final String field, Object value, FilterType filterType, SortingType sortingType) {
        return CompletableFuture.supplyAsync(() -> {
            final List<V> values = new ArrayList<>();
            if (!filterType.isApplicable(value.getClass())) {
                log.warning("Filter type " + filterType.name() + " is not applicable to " + value.getClass().getSimpleName());
                return values;
            }

            switch (filterType) {
                case EQUALS -> this._equals(field, value, values);
                case CONTAINS -> this._contains(field, value, values);
                case STARTS_WITH -> this.startsWith(field, value, values);
                case ENDS_WITH -> this.endsWith(field, value, values);
                case GREATER_THAN -> this.greaterThan(field, value, values);
                case LESS_THAN -> this.lessThan(field, value, values);
                case GREATER_THAN_OR_EQUAL_TO -> this.greaterThanOrEqualTo(field, value, values);
                case LESS_THAN_OR_EQUAL_TO -> this.lessThanOrEqualTo(field, value, values);
                case NOT_EQUALS -> this.notEquals(field, value, values);
                case NOT_CONTAINS -> this.notContains(field, value, values);
                case NOT_STARTS_WITH -> this.notStartsWith(field, value, values);
                case NOT_ENDS_WITH -> this.notEndsWith(field, value, values);
            }

            for (V v : values) {
                cache.put((K) IdUtils.getId(value(), v), v);
            }

            return values;
        });
    }

    @Override
    public CompletableFuture<V> get(K key) {
        if (cache.getIfPresent(key) != null) {
            return CompletableFuture.completedFuture(cache.getIfPresent(key));
        }
        return getFirst(IdUtils.getIdName(this.valueClass), key);
    }

    @Override
    public CompletableFuture<V> getFirst(String field, Object value, FilterType filterType) {
        return CompletableFuture.supplyAsync(() ->
                this.get(field, value, filterType, SortingType.NONE).join().stream().findFirst().orElse(null)
        );
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
            this.cache.invalidate((K) IdUtils.getId(value(), value));
            String field = idField.getName();
            this.execute("DELETE FROM " + this.table + " WHERE " + field + " = '" + IdUtils.getId(value(), value) + "';");
        });
    }

    @Override
    @SneakyThrows
    public CompletableFuture<Void> write() {
        return CompletableFuture.runAsync(() -> {
            this.saveAll(this.cache.asMap().values());
        });
    }

    @Override
    public CompletableFuture<Void> close() {
        return CompletableFuture.runAsync(this.ds::close);
    }

    @Override
    public CompletableFuture<Collection<V>> allValues() {
        return CompletableFuture.supplyAsync(() -> {
            final List<V> values = new ArrayList<>();
            query("SELECT * FROM " + this.table + ";", statement -> {
            }, resultSet -> {
                try {
                    while (resultSet.next()) {
                        values.add(Constants.getGson().fromJson(resultSet.getString("data"), this.valueClass));
                    }
                } catch (final SQLException e) {
                    e.printStackTrace();
                }
            });

            return values;
        });
    }

    @Override
    public void createTable() {
        String idName = IdUtils.getIdName(value());
        boolean isUUID = IdUtils.getIdClass(value()).isAssignableFrom(UUID.class);
        String idType = isUUID ? "VARCHAR(36) NOT NULL" : "VARCHAR(255) NOT NULL";
        idType = idName + " " + idType + " PRIMARY KEY";

        execute("CREATE TABLE IF NOT EXISTS " + table() + "(" + idType + ", data TEXT NOT NULL);");
    }

    @Override
    public void _equals(String field, Object value, List<V> values) {
        query("SELECT * FROM " + table() + " WHERE json_extract(data, '$." + field + "') = ?;", statement -> {
            setStatement(statement, 1, value);
        }, resultSet -> {
            try {
                while (resultSet.next()) {
                    String data = resultSet.getString("data");
                    V v = Constants.getGson().fromJson(data, value());
                    values.add(v);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public void _contains(String field, Object value, List<V> values) {
        query("SELECT * FROM " + table() + " WHERE json_extract(data, '$." + field + "') LIKE ?;", statement -> {
            setStatement(statement, 1, "%" + value + "%");
        }, resultSet -> {
            try {
                while (resultSet.next()) {
                    String data = resultSet.getString("data");
                    V v = Constants.getGson().fromJson(data, value());
                    values.add(v);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public void startsWith(String field, Object value, List<V> values) {
        query("SELECT * FROM " + table() + " WHERE json_extract(data, '$." + field + "') LIKE ?;", statement -> {
            setStatement(statement, 1, value + "%");
        }, resultSet -> {
            try {
                while (resultSet.next()) {
                    String data = resultSet.getString("data");
                    V v = Constants.getGson().fromJson(data, value());
                    values.add(v);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public void endsWith(String field, Object value, List<V> values) {
        query("SELECT * FROM " + table() + " WHERE json_extract(data, '$." + field + "') LIKE ?;", statement -> {
            setStatement(statement, 1, "%" + value);
        }, resultSet -> {
            try {
                while (resultSet.next()) {
                    String data = resultSet.getString("data");
                    V v = Constants.getGson().fromJson(data, value());
                    values.add(v);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public void greaterThan(String field, Object value, List<V> values) {
        query("SELECT * FROM " + table() + " WHERE json_extract(data, '$." + field + "') > ?;", statement -> {
            setStatement(statement, 1, value);
        }, resultSet -> {
            try {
                while (resultSet.next()) {
                    String data = resultSet.getString("data");
                    V v = Constants.getGson().fromJson(data, value());
                    values.add(v);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public void greaterThanOrEqualTo(String field, Object value, List<V> values) {
        query("SELECT * FROM " + table() + " WHERE json_extract(data, '$." + field + "') >= ?;", statement -> {
            setStatement(statement, 1, value);
        }, resultSet -> {
            try {
                while (resultSet.next()) {
                    String data = resultSet.getString("data");
                    V v = Constants.getGson().fromJson(data, value());
                    values.add(v);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public void lessThan(String field, Object value, List<V> values) {
        query("SELECT * FROM " + table() + " WHERE json_extract(data, '$." + field + "') < ?;", statement -> {
            setStatement(statement, 1, value);
        }, resultSet -> {
            try {
                while (resultSet.next()) {
                    String data = resultSet.getString("data");
                    V v = Constants.getGson().fromJson(data, value());
                    values.add(v);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public void lessThanOrEqualTo(String field, Object value, List<V> values) {
        query("SELECT * FROM " + table() + " WHERE json_extract(data, '$." + field + "') <= ?;", statement -> {
            setStatement(statement, 1, value);
        }, resultSet -> {
            try {
                while (resultSet.next()) {
                    String data = resultSet.getString("data");
                    V v = Constants.getGson().fromJson(data, value());
                    values.add(v);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public void notEquals(String field, Object value, List<V> values) {
        query("SELECT * FROM " + table() + " WHERE json_extract(data, '$." + field + "') != ?;", statement -> {
            setStatement(statement, 1, value);
        }, resultSet -> {
            try {
                while (resultSet.next()) {
                    String data = resultSet.getString("data");
                    V v = Constants.getGson().fromJson(data, value());
                    values.add(v);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public void notContains(String field, Object value, List<V> values) {
        query("SELECT * FROM " + table() + " WHERE json_extract(data, '$." + field + "') NOT LIKE ?;", statement -> {
            setStatement(statement, 1, "%" + value + "%");
        }, resultSet -> {
            try {
                while (resultSet.next()) {
                    String data = resultSet.getString("data");
                    V v = Constants.getGson().fromJson(data, this.valueClass);
                    values.add(v);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public void notEndsWith(String field, Object value, List<V> values) {
        query("SELECT * FROM " + table() + " WHERE json_extract(data, '$." + field + "') NOT LIKE ?;", statement -> {
            setStatement(statement, 1, "%" + value);
        }, resultSet -> {
            try {
                while (resultSet.next()) {
                    String data = resultSet.getString("data");
                    V v = Constants.getGson().fromJson(data, this.valueClass);
                    values.add(v);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public void notStartsWith(String field, Object value, List<V> values) {
        query("SELECT * FROM " + table() + " WHERE json_extract(data, '$." + field + "') NOT LIKE ?;", statement -> {
            setStatement(statement, 1, value + "%");
        }, resultSet -> {
            try {
                while (resultSet.next()) {
                    String data = resultSet.getString("data");
                    V v = Constants.getGson().fromJson(data, this.valueClass);
                    values.add(v);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public CompletableFuture<Void> save(V value) {
        return CompletableFuture.runAsync(() -> {
            Object id = IdUtils.getId(value(), value);
            if (id == null) {
                logger().warning("Could not find id field for " + value().getName());
                return;
            }

            String idName = IdUtils.getIdName(value());
            String json = Constants.getGson().toJson(value);
            executeUpdate("INSERT INTO " + table() + " (" + idName + ", data) VALUES (?, ?) ON CONFLICT(" + idName + ") DO UPDATE SET " + idName + " = ?, data = ?;", statement -> {
                statement.setString(1, id.toString());
                statement.setString(2, json);
                statement.setString(3, id.toString());
                statement.setString(4, json);
            });
        });
    }
}
