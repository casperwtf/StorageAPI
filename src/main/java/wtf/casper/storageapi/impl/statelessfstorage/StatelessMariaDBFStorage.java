package wtf.casper.storageapi.impl.statelessfstorage;

import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.intellij.lang.annotations.Language;
import wtf.casper.storageapi.*;
import wtf.casper.storageapi.id.utils.IdUtils;
import wtf.casper.storageapi.misc.ConstructableValue;
import wtf.casper.storageapi.utils.StorageAPIConstants;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class StatelessMariaDBFStorage<K, V> implements StatelessFieldStorage<K, V>, ConstructableValue<K, V> {
    private final Class<K> keyClass;
    private final Class<V> valueClass;
    private final String idFieldName;
    private final HikariDataSource ds;
    private final String table;

    public StatelessMariaDBFStorage(final Class<K> keyClass, final Class<V> valueClass, Credentials credentials) {
        this(keyClass, valueClass, credentials.getTable(), credentials.getHost(), credentials.getPort(-1), credentials.getDatabase(), credentials.getUsername(), credentials.getPassword());
    }

    public StatelessMariaDBFStorage(final Class<K> keyClass, final Class<V> valueClass, final String table, final String host, final int port, final String database, final String username, final String password) {
        this.keyClass = keyClass;
        this.valueClass = valueClass;
        this.idFieldName = IdUtils.getIdName(this.valueClass);
        this.ds = new HikariDataSource();
        this.table = table;
        this.ds.setMaximumPoolSize(20);
        this.ds.setDriverClassName("org.mariadb.jdbc.Driver");
        this.ds.setJdbcUrl("jdbc:mariadb://" + host + ":" + port + "/" + database + "?allowPublicKeyRetrieval=true&autoReconnect=true&useSSL=false");
        this.ds.addDataSourceProperty("user", username);
        this.ds.addDataSourceProperty("password", password);
        this.ds.setAutoCommit(true);
        createTable();
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
    public CompletableFuture<Collection<V>> get(int skip, int limit, Filter... filters) {
        return CompletableFuture.supplyAsync(() -> {
            if (filters.length == 0) {
                return allValues().join();
            }

            List<V> values = new ArrayList<>();
            StringBuilder query = new StringBuilder("SELECT * FROM ").append(table);

            query.append(" WHERE ");
            List<List<Filter>> groups = Filter.group(filters);
            for (List<Filter> group : groups) {
                query.append("(");
                for (Filter filter : group) {
                    query.append("JSON_EXTRACT(data, '$.").append(filter.key()).append("') ").append(getSqlOperator(filter)).append(" AND ");
                }
                query.setLength(query.length() - 5); // Remove the last " AND "
                query.append(") OR ");
            }
            query.setLength(query.length() - 4); // Remove the last " OR "

            if (skip > 0) {
                query.append(" OFFSET ").append(skip);
            }

            if (limit > 0) {
                query.append(" LIMIT ").append(limit);
            }

            Filter sortFilter = filters[0];
            if (sortFilter != null && sortFilter.sortingType() == SortingType.ASCENDING) {
                query.append(" ORDER BY JSON_EXTRACT(data, '$.").append(sortFilter.key()).append("') ASC");
            } else if (sortFilter != null && sortFilter.sortingType() == SortingType.DESCENDING) {
                query.append(" ORDER BY JSON_EXTRACT(data, '$.").append(sortFilter.key()).append("') DESC");
            }
            
            try (Connection connection = ds.getConnection();
                 PreparedStatement stmt = connection.prepareStatement(query.toString())) {
                int index = 1;
                for (Filter filter : filters) {
                    stmt.setObject(index++, filter.value());
                }
                ResultSet rs = stmt.executeQuery();
                while (rs.next()) {
                    values.add(StorageAPIConstants.getGson().fromJson(rs.getString("data"), valueClass));
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return values;

        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<V> get(K key) {
        return CompletableFuture.supplyAsync(() -> {
            String query = "SELECT * FROM " + table + " WHERE " + idFieldName + " = ?";
            try (Connection connection = ds.getConnection();
                 PreparedStatement stmt = connection.prepareStatement(query)) {
                stmt.setObject(1, key);
                ResultSet rs = stmt.executeQuery();
                if (rs.next()) {
                    return StorageAPIConstants.getGson().fromJson(rs.getString("data"), valueClass);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return null;
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Void> save(V value) {
        return CompletableFuture.supplyAsync(() -> {
            String query = "REPLACE INTO " + table + " (" + idFieldName + ", data) VALUES (?, ?)";
            try (Connection connection = ds.getConnection();
                 PreparedStatement stmt = connection.prepareStatement(query)) {
                stmt.setObject(1, uuidToString(IdUtils.getId(valueClass, value)));
                stmt.setString(2, StorageAPIConstants.getGson().toJson(value));
                stmt.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return null;
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Void> remove(V key) {
        return CompletableFuture.supplyAsync(() -> {
            String query = "DELETE FROM " + table + " WHERE " + idFieldName + " = ?";
            try (Connection connection = ds.getConnection();
                 PreparedStatement stmt = connection.prepareStatement(query)) {
                stmt.setObject(1, uuidToString(IdUtils.getId(valueClass, key)));
                stmt.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return null;
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Void> write() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> deleteAll() {
        return CompletableFuture.supplyAsync(() -> {
            String query = "DELETE FROM " + table;
            try (Connection connection = ds.getConnection();
                 PreparedStatement stmt = connection.prepareStatement(query)) {
                stmt.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return null;
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Boolean> contains(String field, Object value) {
        return CompletableFuture.supplyAsync(() -> {
            String query = "SELECT 1 FROM " + table + " WHERE JSON_EXTRACT(data, '$." + field + "') = ?";
            try (Connection connection = ds.getConnection();
                 PreparedStatement stmt = connection.prepareStatement(query)) {
                stmt.setObject(1, value);
                ResultSet rs = stmt.executeQuery();
                return rs.next();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return false;
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Collection<V>> allValues() {
        return CompletableFuture.supplyAsync(() -> {
            List<V> values = new ArrayList<>();
            String query = "SELECT * FROM " + table;
            try (Connection connection = ds.getConnection();
                 PreparedStatement stmt = connection.prepareStatement(query)) {
                ResultSet rs = stmt.executeQuery();
                while (rs.next()) {
                    values.add(StorageAPIConstants.getGson().fromJson(rs.getString("data"), valueClass));
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return values;
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Void> addIndex(String field) {
        return CompletableFuture.runAsync(() -> {
            String query = "ALTER TABLE " + table + " ADD COLUMN " + field + " TEXT AS (JSON_VALUE(data, '$." + field + "')) VIRTUAL";
            try (Connection connection = ds.getConnection();
                 PreparedStatement stmt = connection.prepareStatement(query)) {
                stmt.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public CompletableFuture<Void> removeIndex(String field) {
        return CompletableFuture.runAsync(() -> {
            String query = "ALTER TABLE " + table + " DROP COLUMN " + field;
            try (Connection connection = ds.getConnection();
                 PreparedStatement stmt = connection.prepareStatement(query)) {
                stmt.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }

    private void createTable() {
        try (Connection connection = ds.getConnection();
             Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " + table + " (" + idFieldName + " VARCHAR(255) PRIMARY KEY, data TEXT)");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private String getSqlOperator(Filter filter) {
        switch (filter.filterType()) {
            case ENDS_WITH -> {
                return "LIKE CONCAT('\"%', ?, '\"')";
            }
            case NOT_ENDS_WITH -> {
                return "NOT LIKE CONCAT('\"%', ?, '\"')";
            }
            case STARTS_WITH -> {
                return "LIKE CONCAT('\"', ?, '%\"')";
            }
            case NOT_STARTS_WITH -> {
                return "NOT LIKE CONCAT('\"', ?, '%\"')";
            }
            case CONTAINS -> {
                return "LIKE CONCAT('%', ?, '%')";
            }
            case NOT_CONTAINS -> {
                return "NOT LIKE CONCAT('%', ?, '%')";
            }
            case LESS_THAN, NOT_GREATER_THAN_OR_EQUAL_TO -> {
                return "< ?";
            }
            case EQUALS -> {
                return "= ?";
            }
            case GREATER_THAN, NOT_LESS_THAN_OR_EQUAL_TO -> {
                return "> ?";
            }
            case LESS_THAN_OR_EQUAL_TO, NOT_GREATER_THAN -> {
                return "<= ?";
            }
            case GREATER_THAN_OR_EQUAL_TO, NOT_LESS_THAN -> {
                return ">= ?";
            }
            case NOT_EQUALS -> {
                return "!= ?";
            }
            default -> throw new IllegalArgumentException("Unknown filter type: " + filter.filterType());
        }
    }

    private Object uuidToString(Object uuid) {
        if (uuid instanceof UUID) {
            return uuid.toString();
        } else {
            return uuid;
        }
    }
}