package wtf.casper.storageapi.impl.fstorage;

import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
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
public class MariaDBFStorage<K, V> implements FieldStorage<K, V>, ConstructableValue<K, V> {
    private final Class<K> keyClass;
    private final Class<V> valueClass;
    private final String idFieldName;
    private final HikariDataSource ds;
    private final String table;

    public MariaDBFStorage(final Class<K> keyClass, final Class<V> valueClass, Credentials credentials) {
        this(keyClass, valueClass, credentials.getTable(), credentials.getHost(), credentials.getPort(-1), credentials.getDatabase(), credentials.getUsername(), credentials.getPassword());
    }

    public MariaDBFStorage(final Class<K> keyClass, final Class<V> valueClass, final String table, final String host, final int port, final String database, final String username, final String password) {
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
    public CompletableFuture<Collection<V>> get(Query query) {
        return CompletableFuture.supplyAsync(() -> {
            List<V> values = new ArrayList<>();
            StringBuilder builder = new StringBuilder("SELECT * FROM ").append(table);

            if (!query.conditions().isEmpty()) {
                builder.append(" WHERE ");
                List<List<Condition>> groups = Condition.group(query.conditions().toArray(new Condition[0]));
                for (List<Condition> group : groups) {
                    builder.append("(");
                    for (Condition condition : group) {
                        builder.append("JSON_EXTRACT(data, '$.").append(condition.key()).append("') ").append(getSqlOperator(condition)).append(" AND ");
                    }
                    builder.setLength(builder.length() - 5); // Remove the last " AND "
                    builder.append(") OR ");
                }
                builder.setLength(builder.length() - 4); // Remove the last " OR "
            }

            if (query.offset() > 0) {
                builder.append(" OFFSET ").append(query.offset());
            }

            if (query.limit() > 0) {
                builder.append(" LIMIT ").append(query.limit());
            }

            Sort sortCondition = query.sorts().get(0);
            if (sortCondition != null && sortCondition.sortingType() == SortingType.ASCENDING) {
                builder.append(" ORDER BY JSON_EXTRACT(data, '$.").append(sortCondition.field()).append("') ASC");
            } else if (sortCondition != null && sortCondition.sortingType() == SortingType.DESCENDING) {
                builder.append(" ORDER BY JSON_EXTRACT(data, '$.").append(sortCondition.field()).append("') DESC");
            }

            try (Connection connection = ds.getConnection();
                 PreparedStatement stmt = connection.prepareStatement(builder.toString())) {
                int index = 1;
                for (Condition condition : query.conditions()) {
                    stmt.setObject(index++, condition.value());
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
    public CompletableFuture<Void> remove(Query query) {
        return CompletableFuture.runAsync(() -> {
            StringBuilder builder = new StringBuilder("DELETE FROM ").append(table);

            if (!query.conditions().isEmpty()) {
                builder.append(" WHERE ");
                List<List<Condition>> groups = Condition.group(query.conditions().toArray(new Condition[0]));
                for (List<Condition> group : groups) {
                    builder.append("(");
                    for (Condition condition : group) {
                        builder.append("JSON_EXTRACT(data, '$.").append(condition.key()).append("') ").append(getSqlOperator(condition)).append(" AND ");
                    }
                    builder.setLength(builder.length() - 5); // Remove the last " AND "
                    builder.append(") OR ");
                }
                builder.setLength(builder.length() - 4); // Remove the last " OR "
            }

            if (query.offset() > 0) {
                builder.append(" OFFSET ").append(query.offset());
            }

            if (query.limit() > 0) {
                builder.append(" LIMIT ").append(query.limit());
            }


            try (Connection connection = ds.getConnection();
                 PreparedStatement stmt = connection.prepareStatement(builder.toString())) {
                int index = 1;
                for (Condition condition : query.conditions()) {
                    stmt.setObject(index++, condition.value());
                }
                stmt.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }, StorageAPIConstants.DB_THREAD_POOL);
    }

    @Override
    public CompletableFuture<List<AggregationResult>> aggregate(Query query) {
        return CompletableFuture.supplyAsync(() -> {
            if (query.aggregations().isEmpty()) {
                throw new IllegalArgumentException("At least one aggregation must be specified");
            }

            StringBuilder builder = new StringBuilder("SELECT ");

            for (Aggregation aggregation : query.aggregations()) {
                builder.append(getSqlAggregator(aggregation)).append(", ");
            }
            builder.setLength(builder.length() - 2);
            builder.append(" FROM ").append(table);

            if (!query.conditions().isEmpty()) {
                builder.append(" WHERE ");
                List<List<Condition>> groups = Condition.group(query.conditions().toArray(new Condition[0]));
                for (List<Condition> group : groups) {
                    builder.append("(");
                    for (Condition condition : group) {
                        builder.append("JSON_EXTRACT(data, '$.").append(condition.key()).append("') ").append(getSqlOperator(condition)).append(" AND ");
                    }
                    builder.setLength(builder.length() - 5); // Remove the last " AND "
                    builder.append(") OR ");
                }
                builder.setLength(builder.length() - 4); // Remove the last " OR "
            }

            if (query.offset() > 0) {
                builder.append(" OFFSET ").append(query.offset());
            }

            if (query.limit() > 0) {
                builder.append(" LIMIT ").append(query.limit());
            }

            List<AggregationResult> results = new ArrayList<>();
            try (Connection connection = ds.getConnection();
                 PreparedStatement stmt = connection.prepareStatement(builder.toString())) {
                int index = 1;
                for (Condition condition : query.conditions()) {
                    stmt.setObject(index++, condition.value());
                }
                ResultSet rs = stmt.executeQuery();
                while (rs.next()) {
                    Object object = rs.getObject(0);
                    String alias = rs.getCursorName();
                    results.add(new AggregationResult(alias, object));
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }

            return results;
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
    public CompletableFuture<Void> index(String field) {
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
    public CompletableFuture<Void> unindex(String field) {
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

    private String getSqlOperator(Condition condition) {
        switch (condition.conditionType()) {
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
            default -> throw new IllegalArgumentException("Unknown filter type: " + condition.conditionType());
        }
    }

    public String getSqlAggregator(Aggregation aggregation) {
        switch (aggregation.function()) {
            case COUNT -> {
                return "COUNT(*)" + (aggregation.alias() == null ? "" : " AS " + aggregation.alias());
            }
            case SUM -> {
                return "SUM(JSON_VALUE(data, '$." + aggregation.field() + "'))" + (aggregation.alias() == null ? "" : " AS " + aggregation.alias());
            }
            case AVG -> {
                return "AVG(JSON_VALUE(data, '$." + aggregation.field() + "'))" + (aggregation.alias() == null ? "" : " AS " + aggregation.alias());
            }
            case MAX -> {
                return "MAX(JSON_VALUE(data, '$." + aggregation.field() + "'))" + (aggregation.alias() == null ? "" : " AS " + aggregation.alias());
            }
            case MIN -> {
                return "MIN(JSON_VALUE(data, '$." + aggregation.field() + "'))" + (aggregation.alias() == null ? "" : " AS " + aggregation.alias());
            }
            default -> {
                throw new IllegalArgumentException("Unknown aggregation function: " + aggregation.function());
            }
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