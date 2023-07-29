package wtf.casper.storageapi.misc;

import com.zaxxer.hikari.HikariDataSource;
import lombok.SneakyThrows;
import wtf.casper.storageapi.StatelessFieldStorage;
import wtf.casper.storageapi.id.StorageSerialized;
import wtf.casper.storageapi.id.Transient;
import wtf.casper.storageapi.id.utils.IdUtils;
import wtf.casper.storageapi.utils.ReflectionUtil;
import wtf.casper.storageapi.utils.UnsafeConsumer;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

public interface ISQLStorage<K, V> extends StatelessFieldStorage<K, V>, ConstructableValue<K, V>, KeyValue<K, V> {

    HikariDataSource getDataSource();

    String getTable();

    Logger logger();

    default CompletableFuture<ResultSet> query(final String query, final UnsafeConsumer<PreparedStatement> statement, final UnsafeConsumer<ResultSet> result) {
        return CompletableFuture.supplyAsync(() -> {
            try (final Connection connection = this.getDataSource().getConnection()) {
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
        try (final Connection connection = this.getDataSource().getConnection()) {
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
        try (final Connection connection = this.getDataSource().getConnection()) {
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
        try (final Connection connection = this.getDataSource().getConnection()) {
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

    default void addColumn(final String column, final String type) {
        this.execute("ALTER TABLE " + getTable() + " ADD COLUMN " + column + " " + type + ";");
    }

    /**
     * Will scan the class for fields and add them to the database if they don't exist
     */
    default void scanForMissingColumns() {
        List<Field> fields = Arrays.stream(this.value().getDeclaredFields())
                .filter(field -> !field.isAnnotationPresent(Transient.class))
                .filter(field -> !Modifier.isTransient(field.getModifiers()))
                .toList();

        for (Field declaredField : fields) {
            final String name = declaredField.getName();
            final String type = this.getType(declaredField.getType());

            this.query("SELECT * FROM " + getTable() + " LIMIT 1;", resultSet -> {
                try {
                    if (resultSet.findColumn(name) == 0) {
                        this.addColumn(name, type);
                    }
                } catch (SQLException e) {
                    this.addColumn(name, type);
                }

                resultSet.close();
            });
        }
    }

    /**
     * Generate an SQL Script to create the table based on the class
     */
    default String createTableFromObject() {
        final StringBuilder builder = new StringBuilder();

        List<Field> fields = Arrays.stream(this.value().getDeclaredFields())
                .filter(field -> !field.isAnnotationPresent(Transient.class))
                .filter(field -> !Modifier.isTransient(field.getModifiers()))
                .toList();

        if (fields.size() == 0) {
            return "";
        }

        builder.append("CREATE TABLE IF NOT EXISTS ").append(getTable()).append(" (");

        String idName = IdUtils.getIdName(value());

        int index = 0;
        for (Field declaredField : fields) {

            final String name = declaredField.getName();
            String type = this.getType(declaredField.getType());

            if (declaredField.isAnnotationPresent(StorageSerialized.class)) {
                builder.append(createSubTables(declaredField.getType(), declaredField.getName() + "."));
                continue;
            }

            builder.append("`").append(name).append("`").append(" ").append(type);
            if (name.equals(idName)) {
                builder.append(" PRIMARY KEY");
            }

            index++;

            if (index != fields.size()) {
                builder.append(", ");
            }

        }
        builder.append(");");
        System.out.println(builder.toString());
        return builder.toString();
    }

    default String createSubTables(Class<?> clazz, String parentPrefix) {
        List<Field> fields = Arrays.stream(this.value().getDeclaredFields())
                .filter(field -> !field.isAnnotationPresent(Transient.class))
                .filter(field -> !Modifier.isTransient(field.getModifiers()))
                .toList();

        if (fields.isEmpty()) {
            return "";
        }

        StringBuilder builder = new StringBuilder();
        for (Field declaredField : fields) {

            final String name = declaredField.getName();
            String type = this.getType(declaredField.getType());

            if (declaredField.isAnnotationPresent(StorageSerialized.class)) {
                builder.append(createSubTables(declaredField.getType(), parentPrefix + declaredField.getName() + "."));
                continue;
            }

            builder.append("`").append(parentPrefix).append(name).append("`").append(" ").append(type);
        }

        return builder.toString();
    }

    /**
     * This takes an SQL Result Set and parses it into an object
     */
    @SneakyThrows
    default V construct(final ResultSet resultSet) {
        final V value = constructValue();
        List<Field> declaredFields = Arrays.stream(value().getDeclaredFields())
                .filter(field -> !field.isAnnotationPresent(Transient.class))
                .filter(field -> !Modifier.isTransient(field.getModifiers()))
                .toList();

        for (Field declaredField : declaredFields) {
            if (declaredField.isAnnotationPresent(StorageSerialized.class)) {
                final String name = declaredField.getName();
                final String string = resultSet.getString(name);
                final Object object = StorageGson.getGson().fromJson(string, declaredField.getType());
                declaredField.setAccessible(true);
                declaredField.set(value, object);
                continue;
            }

            final String name = declaredField.getName();
            final Object object = resultSet.getObject(name);

            if (declaredField.getType() == UUID.class && object instanceof String) {
                ReflectionUtil.setPrivateField(value, name, UUID.fromString((String) object));
                continue;
            } else if (declaredField.getType().isEnum() && object instanceof String) {
                Enum<?> enumValue = Enum.valueOf((Class<? extends Enum>) declaredField.getType(), (String) object);
                ReflectionUtil.setPrivateField(value, name, enumValue);
                continue;
            }

            ReflectionUtil.setPrivateField(value, name, object);
        }

        return value;
    }

    default String getUpdateValues() {
        final StringBuilder builder = new StringBuilder();
        int i = 0;

        List<Field> fields = Arrays.stream(value().getDeclaredFields())
                .filter(field -> !field.isAnnotationPresent(Transient.class))
                .filter(field -> !Modifier.isTransient(field.getModifiers()))
                .toList();

        for (final Field field : fields) {
            builder.append("`").append(field.getName()).append("` = excluded.`").append(field.getName()).append("`");

            if (i != fields.size() - 1) {
                builder.append(", ");
            }

            i++;
        }

        return builder.toString();
    }

    /**
     * Generates an SQL String for the columns associated with a value class.
     */
    default String getColumns() {
        final StringBuilder builder = new StringBuilder();

        List<Field> fields = Arrays.stream(value().getDeclaredFields())
                .filter(field -> !field.isAnnotationPresent(Transient.class))
                .filter(field -> !Modifier.isTransient(field.getModifiers()))
                .toList();

        for (final Field field : fields) {
            builder.append("`").append(field.getName()).append("`").append(",");
        }

        return builder.substring(0, builder.length() - 1);
    }


    /**
     * Converts a Java class to an SQL type.
     */
    default String getType(Class<?> type) {
        return switch (type.getName()) {
            case "java.lang.String" -> "VARCHAR(255)";
            case "java.lang.Integer", "int" -> "INT";
            case "java.lang.Long", "long" -> "BIGINT";
            case "java.lang.Boolean", "boolean" -> "BOOLEAN";
            case "java.lang.Double", "double" -> "DOUBLE";
            case "java.lang.Float", "float" -> "FLOAT";
            case "java.lang.Short", "short" -> "SMALLINT";
            case "java.lang.Byte", "byte" -> "TINYINT";
            case "java.lang.Character", "char" -> "CHAR";
            case "java.util.UUID" -> "VARCHAR(36)";
            default -> "VARCHAR(255)";
        };
    }

    default String getValues(Object value, Class<?> clazz) {
        return getValues(value, clazz, false);
    }

    /**
     * Generates an SQL String for inserting a value into the database.
     */
    default String getValues(Object value, Class<?> clazz, boolean subObject) {
        final StringBuilder builder = new StringBuilder();
        int i = 0;

        List<Field> fields = Arrays.stream(clazz.getDeclaredFields())
                .filter(field -> !field.isAnnotationPresent(Transient.class))
                .filter(field -> !Modifier.isTransient(field.getModifiers()))
                .filter(field -> !Modifier.isStatic(field.getModifiers()))
                .toList();

        for (final Field field : fields) {
            Object privateField = ReflectionUtil.getPrivateField(value, field.getName());
            if (field.isAnnotationPresent(StorageSerialized.class)) {
                builder.append(getValues(privateField, privateField.getClass(), true));
            } else {
                boolean shouldHaveQuotes = shouldHaveQuotes(privateField);
                if (shouldHaveQuotes) {
                    builder.append("'");
                }
                if (privateField instanceof Map<?, ?> map) {
                    if (map.isEmpty()) {
                        builder.append("NULL");
                    }
                } else if (privateField instanceof List<?> list) {
                    if (list.isEmpty()) {
                        builder.append("''");
                    }
                } else {
                    builder.append(privateField);
                }

                if (shouldHaveQuotes) {
                    builder.append("'");
                }
            }
            if (i != fields.size() - 1) {
                builder.append(", ");
            }
            i++;
        }

        return builder.toString();
    }

    default boolean shouldHaveQuotes(Object value) {
        if (value instanceof Enum<?>) {
            return true;
        }
        return switch (value.getClass().getName()) {
            case "java.lang.String", "java.util.UUID" -> true;
            default -> false;
        };
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

    default void _equals(String field, Object value, List<V> values) {
        this.query("SELECT * FROM " + getTable() + " WHERE " + field + " = ?", preparedStatement -> {
            setStatement(preparedStatement, 1, value);
        }, resultSet -> {
            while (resultSet.next()) {
                values.add(this.construct(resultSet));
            }
            resultSet.close();
        }).join();
    }

    default void _contains(String field, Object value, List<V> values) {
        this.query("SELECT * FROM " + getTable() + " WHERE " + field + " LIKE ?", preparedStatement -> {
            setStatement(preparedStatement, 1, "%" + value + "%");
        }, resultSet -> {
            while (resultSet.next()) {
                values.add(this.construct(resultSet));
            }
            resultSet.close();
        }).join();
    }

    default void startsWith(String field, Object value, List<V> values) {
        this.query("SELECT * FROM " + getTable() + " WHERE " + field + " LIKE ?", preparedStatement -> {
            setStatement(preparedStatement, 1, value + "%");
        }, resultSet -> {
            while (resultSet.next()) {
                values.add(this.construct(resultSet));
            }
            resultSet.close();
        }).join();
    }

    default void endsWith(String field, Object value, List<V> values) {
        this.query("SELECT * FROM " + getTable() + " WHERE " + field + " LIKE ?", preparedStatement -> {
            setStatement(preparedStatement, 1, "%" + value);
        }, resultSet -> {
            while (resultSet.next()) {
                values.add(this.construct(resultSet));
            }
            resultSet.close();
        }).join();
    }

    default void greaterThan(String field, Object value, List<V> values) {
        this.query("SELECT * FROM " + getTable() + " WHERE " + field + " > ?", preparedStatement -> {
            setStatement(preparedStatement, 1, value);
        }, resultSet -> {
            while (resultSet.next()) {
                values.add(this.construct(resultSet));
            }
            resultSet.close();
        }).join();
    }

    default void lessThan(String field, Object value, List<V> values) {
        this.query("SELECT * FROM " + getTable() + " WHERE " + field + " < ?", preparedStatement -> {
            setStatement(preparedStatement, 1, value);
        }, resultSet -> {
            while (resultSet.next()) {
                values.add(this.construct(resultSet));
            }
            resultSet.close();
        }).join();
    }

    default void greaterThanOrEqualTo(String field, Object value, List<V> values) {
        this.query("SELECT * FROM " + getTable() + " WHERE " + field + " >= ?", preparedStatement -> {
            setStatement(preparedStatement, 1, value);
        }, resultSet -> {
            while (resultSet.next()) {
                values.add(this.construct(resultSet));
            }
            resultSet.close();
        }).join();
    }

    default void lessThanOrEqualTo(String field, Object value, List<V> values) {
        this.query("SELECT * FROM " + getTable() + " WHERE " + field + " <= ?", preparedStatement -> {
            setStatement(preparedStatement, 1, value);
        }, resultSet -> {
            while (resultSet.next()) {
                values.add(this.construct(resultSet));
            }
            resultSet.close();
        }).join();
    }

    default void in(String field, Object value, List<V> values) {
        this.query("SELECT * FROM " + getTable() + " WHERE " + field + " IN (?)", preparedStatement -> {
            setStatement(preparedStatement, 1, value);
        }, resultSet -> {
            while (resultSet.next()) {
                values.add(this.construct(resultSet));
            }
            resultSet.close();
        }).join();
    }

    default void notEquals(String field, Object value, List<V> values) {
        this.query("SELECT * FROM " + getTable() + " WHERE " + field + " != ?", preparedStatement -> {
            setStatement(preparedStatement, 1, value);
        }, resultSet -> {
            while (resultSet.next()) {
                values.add(this.construct(resultSet));
            }
            resultSet.close();
        }).join();
    }

    default void notContains(String field, Object value, List<V> values) {
        this.query("SELECT * FROM " + getTable() + " WHERE " + field + " NOT LIKE ?", preparedStatement -> {
            setStatement(preparedStatement, 1, "%" + value + "%");
        }, resultSet -> {
            while (resultSet.next()) {
                values.add(this.construct(resultSet));
            }
            resultSet.close();
        }).join();
    }

    default void notStartsWIth(String field, Object value, List<V> values) {
        this.query("SELECT * FROM " + getTable() + " WHERE " + field + " NOT LIKE ?", preparedStatement -> {
            setStatement(preparedStatement, 1, value + "%");
        }, resultSet -> {
            while (resultSet.next()) {
                values.add(this.construct(resultSet));
            }
            resultSet.close();
        }).join();
    }

    default void notEndsWith(String field, Object value, List<V> values) {
        this.query("SELECT * FROM " + getTable() + " WHERE " + field + " NOT LIKE ?", preparedStatement -> {
            setStatement(preparedStatement, 1, "%" + value);
        }, resultSet -> {
            while (resultSet.next()) {
                values.add(this.construct(resultSet));
            }
            resultSet.close();
        }).join();
    }

    default void notIn(String field, Object value, List<V> values) {
        this.query("SELECT * FROM " + getTable() + " WHERE " + field + " NOT IN (?)", preparedStatement -> {
            setStatement(preparedStatement, 1, value);
        }, resultSet -> {
            while (resultSet.next()) {
                values.add(this.construct(resultSet));
            }
            resultSet.close();
        }).join();
    }
}
