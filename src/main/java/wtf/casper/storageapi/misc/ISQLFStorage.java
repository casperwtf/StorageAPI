package wtf.casper.storageapi.misc;

import com.zaxxer.hikari.HikariDataSource;
import wtf.casper.storageapi.StatelessFieldStorage;
import wtf.casper.storageapi.id.Transient;
import wtf.casper.storageapi.id.utils.IdUtils;
import wtf.casper.storageapi.utils.Constants;
import wtf.casper.storageapi.utils.UnsafeConsumer;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

public interface ISQLFStorage<K, V> extends StatelessFieldStorage<K, V>, ConstructableValue<K, V> {

    HikariDataSource dataSource();

    String table();

    Logger logger();

    default CompletableFuture<ResultSet> query(final String query, final UnsafeConsumer<PreparedStatement> statement, final UnsafeConsumer<ResultSet> result) {
        return CompletableFuture.supplyAsync(() -> {
            try (final Connection connection = this.dataSource().getConnection()) {
                try (final PreparedStatement prepared = connection.prepareStatement(query)) {
                    statement.accept(prepared);
                    final ResultSet resultSet = prepared.executeQuery();
                    result.accept(resultSet);
                    return resultSet;
                } catch (final SQLException e) {
                    logger().warning("Error while executing query: " + query);
                    e.printStackTrace();
                }
            } catch (final SQLException e) {
                logger().warning("Error while executing query: " + query);
                e.printStackTrace();
            }
            return null;
        });
    }

    default CompletableFuture<ResultSet> query(final String query, final UnsafeConsumer<ResultSet> result) {
        return this.query(query, statement -> {
        }, result);
    }

    default void execute(final String statement) {
        this.execute(statement, ps -> {
        });
    }

    default void execute(final String statement, final UnsafeConsumer<PreparedStatement> consumer) {
        try (final Connection connection = this.dataSource().getConnection()) {
            try (final PreparedStatement prepared = connection.prepareStatement(statement)) {
                consumer.accept(prepared);
                prepared.execute();
            } catch (final SQLException e) {
                logger().warning("Error while executing query: " + statement);
                e.printStackTrace();
            }
        } catch (final SQLException e) {
            logger().warning("Error while executing query: " + statement);
            e.printStackTrace();
        }
    }

    default void executeQuery(final String statement) {
        this.executeQuery(statement, ps -> {
        });
    }

    default void executeQuery(final String statement, final UnsafeConsumer<PreparedStatement> consumer) {
        try (final Connection connection = this.dataSource().getConnection()) {
            try (final PreparedStatement prepared = connection.prepareStatement(statement)) {
                consumer.accept(prepared);
                prepared.executeQuery();
            } catch (final SQLException e) {
                logger().warning("Error while executing query: " + statement);
                e.printStackTrace();
            }
        } catch (final SQLException e) {
            logger().warning("Error while executing query: " + statement);
            e.printStackTrace();
        }
    }

    default void executeUpdate(final String statement) {
        this.executeUpdate(statement, ps -> {
        });
    }

    default void executeUpdate(final String statement, final UnsafeConsumer<PreparedStatement> consumer) {
        try (final Connection connection = this.dataSource().getConnection()) {
            try (final PreparedStatement prepared = connection.prepareStatement(statement)) {
                consumer.accept(prepared);
                prepared.executeUpdate();
            } catch (final SQLException e) {
                logger().warning("Error while executing query: " + statement);
                e.printStackTrace();
            }
        } catch (final SQLException e) {
            logger().warning("Error while executing query: " + statement);
            e.printStackTrace();
        }
    }

    default List<Field> getFields(Class<?> clazz) {
        return Arrays.stream(clazz.getDeclaredFields())
                .filter(field -> !field.isAnnotationPresent(Transient.class))
                .filter(field -> !Modifier.isTransient(field.getModifiers()))
                .filter(field -> !Modifier.isStatic(field.getModifiers()))
                .toList();
    }

    default void createTable() {
        String idName = IdUtils.getIdName(value());
        boolean isUUID = UUID.class.isAssignableFrom(IdUtils.getIdClass(value()));
        String idType = isUUID ? "VARCHAR(36) NOT NULL" : "VARCHAR(255) NOT NULL";
        idType = idName + " " + idType + " PRIMARY KEY";

        execute("CREATE TABLE IF NOT EXISTS " + table() + " (" + idType + ", json JSON NOT NULL);");
    }

    default CompletableFuture<Void> save(V value) {
        return CompletableFuture.runAsync(() -> {
            Object id = IdUtils.getId(value());
            if (id == null) {
                logger().warning("Could not find id field for " + value().getSimpleName());
                return;
            }

            String idName = IdUtils.getIdName(value());
            String json = Constants.getGson().toJson(value);
            executeUpdate("INSERT INTO " + table() + " (" + idName + ", json) VALUES (?, ?) ON DUPLICATE KEY UPDATE json = ?;", statement -> {
                statement.setString(1, id.toString());
                statement.setString(2, json);
            });
        });
    }

    default CompletableFuture<List<V>> getField(String fieldPath) {
        return CompletableFuture.supplyAsync(() -> {
            List<V> values = new ArrayList<>();
            query("SELECT * FROM " + table() + " WHERE json_extract(json, '$." + fieldPath + "') IS NOT NULL;", statement -> {
            }, resultSet -> {
                try {
                    while (resultSet.next()) {
                        String json = resultSet.getString("json");
                        V value = Constants.getGson().fromJson(json, value());
                        values.add(value);
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });
            return values;
        });
    }

    default void _equals(String field, Object value, List<V> values) {
        query("SELECT * FROM " + table() + " WHERE json_extract(json, '$." + field + "') = ?;", statement -> {
            setStatement(statement, 1, value);
        }, resultSet -> {
            try {
                while (resultSet.next()) {
                    String json = resultSet.getString("json");
                    V v = Constants.getGson().fromJson(json, value());
                    values.add(v);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }

    default void _contains(String field, Object value, List<V> values) {
        query("SELECT * FROM " + table() + " WHERE json_extract(json, '$." + field + "') LIKE ?;", statement -> {
            setStatement(statement, 1, "%" + value + "%");
        }, resultSet -> {
            try {
                while (resultSet.next()) {
                    String json = resultSet.getString("json");
                    V v = Constants.getGson().fromJson(json, value());
                    values.add(v);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }

    default void startsWith(String field, Object value, List<V> values) {
        query("SELECT * FROM " + table() + " WHERE json_extract(json, '$." + field + "') LIKE ?;", statement -> {
            setStatement(statement, 1, value + "%");
        }, resultSet -> {
            try {
                while (resultSet.next()) {
                    String json = resultSet.getString("json");
                    V v = Constants.getGson().fromJson(json, value());
                    values.add(v);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }

    default void endsWith(String field, Object value, List<V> values) {
        query("SELECT * FROM " + table() + " WHERE json_extract(json, '$." + field + "') LIKE ?;", statement -> {
            setStatement(statement, 1, "%" + value);
        }, resultSet -> {
            try {
                while (resultSet.next()) {
                    String json = resultSet.getString("json");
                    V v = Constants.getGson().fromJson(json, value());
                    values.add(v);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }

    default void greaterThan(String field, Object value, List<V> values) {
        query("SELECT * FROM " + table() + " WHERE json_extract(json, '$." + field + "') > ?;", statement -> {
            setStatement(statement, 1, value);
        }, resultSet -> {
            try {
                while (resultSet.next()) {
                    String json = resultSet.getString("json");
                    V v = Constants.getGson().fromJson(json, value());
                    values.add(v);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }

    default void lessThan(String field, Object value, List<V> values) {
        query("SELECT * FROM " + table() + " WHERE json_extract(json, '$." + field + "') < ?;", statement -> {
            setStatement(statement, 1, value);
        }, resultSet -> {
            try {
                while (resultSet.next()) {
                    String json = resultSet.getString("json");
                    V v = Constants.getGson().fromJson(json, value());
                    values.add(v);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }

    default void greaterThanOrEqualTo(String field, Object value, List<V> values) {
        query("SELECT * FROM " + table() + " WHERE json_extract(json, '$." + field + "') >= ?;", statement -> {
            setStatement(statement, 1, value);
        }, resultSet -> {
            try {
                while (resultSet.next()) {
                    String json = resultSet.getString("json");
                    V v = Constants.getGson().fromJson(json, value());
                    values.add(v);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }

    default void lessThanOrEqualTo(String field, Object value, List<V> values) {
        query("SELECT * FROM " + table() + " WHERE json_extract(json, '$." + field + "') <= ?;", statement -> {
            setStatement(statement, 1, value);
        }, resultSet -> {
            try {
                while (resultSet.next()) {
                    String json = resultSet.getString("json");
                    V v = Constants.getGson().fromJson(json, value());
                    values.add(v);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }

    default void notEquals(String field, Object value, List<V> values) {
        query("SELECT * FROM " + table() + " WHERE json_extract(json, '$." + field + "') != ?;", statement -> {
            setStatement(statement, 1, value);
        }, resultSet -> {
            try {
                while (resultSet.next()) {
                    String json = resultSet.getString("json");
                    V v = Constants.getGson().fromJson(json, value());
                    values.add(v);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }

    default void notContains(String field, Object value, List<V> values) {
        query("SELECT * FROM " + table() + " WHERE json_extract(json, '$." + field + "') NOT LIKE ?;", statement -> {
            setStatement(statement, 1, "%" + value + "%");
        }, resultSet -> {
            try {
                while (resultSet.next()) {
                    String json = resultSet.getString("json");
                    V v = Constants.getGson().fromJson(json, value());
                    values.add(v);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }

    default void notStartsWIth(String field, Object value, List<V> values) {
        query("SELECT * FROM " + table() + " WHERE json_extract(json, '$." + field + "') NOT LIKE ?;", statement -> {
            setStatement(statement, 1, value + "%");
        }, resultSet -> {
            try {
                while (resultSet.next()) {
                    String json = resultSet.getString("json");
                    V v = Constants.getGson().fromJson(json, value());
                    values.add(v);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }

    default void notEndsWith(String field, Object value, List<V> values) {
        query("SELECT * FROM " + table() + " WHERE json_extract(json, '$." + field + "') NOT LIKE ?;", statement -> {
            setStatement(statement, 1, "%" + value);
        }, resultSet -> {
            try {
                while (resultSet.next()) {
                    String json = resultSet.getString("json");
                    V v = Constants.getGson().fromJson(json, value());
                    values.add(v);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }

    default void setStatement(PreparedStatement statement, int i, Object value) {
        switch (value.getClass().getSimpleName()) {
            case "String" -> {
                try {
                    statement.setString(i, (String) value);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            case "Integer", "int" -> {
                try {
                    statement.setInt(i, (Integer) value);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            case "Long", "long" -> {
                try {
                    statement.setLong(i, (Long) value);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            case "Boolean", "boolean" -> {
                try {
                    statement.setBoolean(i, (Boolean) value);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            case "Double", "double" -> {
                try {
                    statement.setDouble(i, (Double) value);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            case "Float", "float" -> {
                try {
                    statement.setFloat(i, (Float) value);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            case "Short", "short" -> {
                try {
                    statement.setShort(i, (Short) value);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            case "Byte", "byte" -> {
                try {
                    statement.setByte(i, (Byte) value);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            case "Character", "char" -> {
                try {
                    statement.setString(i, String.valueOf(value));
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            case "UUID" -> {
                try {
                    statement.setString(i, value.toString());
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            case "Timestamp" -> {
                try {
                    statement.setTimestamp(i, (Timestamp) value);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            case "Date" -> {
                try {
                    statement.setDate(i, (Date) value);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            case "Time" -> {
                try {
                    statement.setTime(i, (Time) value);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            case "BigDecimal" -> {
                try {
                    statement.setBigDecimal(i, (BigDecimal) value);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            case "Blob" -> {
                try {
                    statement.setBlob(i, (Blob) value);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            case "Clob" -> {
                try {
                    statement.setClob(i, (Clob) value);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            case "Array" -> {
                try {
                    statement.setArray(i, (Array) value);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            case "Ref" -> {
                try {
                    statement.setRef(i, (Ref) value);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            case "NClob" -> {
                try {
                    statement.setNClob(i, (NClob) value);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            case "NString" -> {
                try {
                    statement.setNString(i, (String) value);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            case "RowId" -> {
                try {
                    statement.setRowId(i, (RowId) value);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            case "SQLXML" -> {
                try {
                    statement.setSQLXML(i, (SQLXML) value);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            default -> {
                try {
                    statement.setObject(i, value);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
