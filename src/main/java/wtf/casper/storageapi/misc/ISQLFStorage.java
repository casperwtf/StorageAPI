package wtf.casper.storageapi.misc;

import com.zaxxer.hikari.HikariDataSource;
import org.checkerframework.checker.units.qual.A;
import wtf.casper.storageapi.StatelessFieldStorage;
import wtf.casper.storageapi.id.StorageSerialized;
import wtf.casper.storageapi.id.Transient;
import wtf.casper.storageapi.id.utils.IdUtils;
import wtf.casper.storageapi.utils.Constants;
import wtf.casper.storageapi.utils.Lazy;
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

    Lazy<List<Field>> getDeclaredFields();

    Lazy<Map<String, String>> getInsertIntoTables();

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

    default List<Field> getSerializableFields(Class<?> clazz) {
        return getDeclaredFields().get().stream()
                .filter(field -> !field.isAnnotationPresent(Transient.class))
                .filter(field -> !Modifier.isStatic(field.getModifiers()))
                .filter(field -> !Modifier.isTransient(field.getModifiers()))
                .toList();
    }

    default void createRequiredTables() {
        Map<String, Class<?>> map = requiredTables(value(), "", new HashMap<>());

        map.forEach((string, aClass) -> execute(createTableFromClass(string, aClass)));
    }

    default Map<String, Class<?>> requiredTables(Class<?> clazz, String parent, Map<String, Class<?>> tables) {
        if (parent == null || parent.isEmpty()) {
            parent = table();
        }

        tables.put(parent, clazz);

        for (Field field : getSerializableFields(clazz)) {
            if (field.getType().isAnnotationPresent(StorageSerialized.class)) {
                tables.putAll(requiredTables(field.getType(), parent + "." + field.getName(), tables));
            }
        }

        return tables;
    }

    default String createTableFromClass(String tableName, Class<?> clazz) {
        StringBuilder builder = new StringBuilder();

        boolean idIsUUID = IdUtils.getIdType(clazz) == UUID.class;

        builder.append("CREATE TABLE IF NOT EXISTS ");
        builder.append(tableName);
        builder.append(" (");
        if (idIsUUID) {
            builder.append("_id VARCHAR(36) PRIMARY KEY, ");
        } else {
            builder.append("_id VARCHAR(255) PRIMARY KEY, ");
        }

        List<Field> fields = getSerializableFields(clazz);
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            if (field.getType().isAnnotationPresent(StorageSerialized.class)) {
                continue;
            }
            builder.append(field.getName());
            builder.append(" ");
            builder.append(getSQLType(field.getType()));
            if (i != fields.size() - 1) {
                builder.append(", ");
            }
        }

        builder.append(");");

        return builder.toString();
    }

    default Map<String, String> insertIntoTables() {
        Map<String, String> map = new HashMap<>();
        insertIntoTables(value(), "", map);
        return map;
    }

    // generate insert into statements to cache
    default void insertIntoTables(Class<?> clazz, String parent, Map<String, String> map) {
        if (parent == null || parent.isEmpty()) {
            parent = table();
        }

        StringBuilder builder = new StringBuilder();
        builder.append("INSERT INTO ");
        builder.append(parent);
        builder.append(" (_id, ");

        List<Field> fields = getSerializableFields(clazz);
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            if (field.getType().isAnnotationPresent(StorageSerialized.class)) {
                continue;
            }
            builder.append(field.getName());
            if (i != fields.size() - 1) {
                builder.append(", ");
            }
        }

        builder.append(") VALUES (?, ");

        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).getType().isAnnotationPresent(StorageSerialized.class)) {
                continue;
            }
            builder.append("?");
            if (i != fields.size() - 1) {
                builder.append(", ");
            }
        }

        builder.append(") ON DUPLICATE KEY UPDATE ");

        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).getType().isAnnotationPresent(StorageSerialized.class)) {
                continue;
            }
            builder.append(fields.get(i).getName());
            builder.append(" = VALUES(");
            builder.append(fields.get(i).getName());
            builder.append(")");
            if (i != fields.size() - 1) {
                builder.append(", ");
            }
        }

        builder.append(";");

        map.put(parent, builder.toString());

        for (Field field : getSerializableFields(clazz)) {
            if (field.getType().isAnnotationPresent(StorageSerialized.class)) {
                insertIntoTables(field.getType(), parent + "." + field.getName(), map);
            }
        }
    }

    default String getSQLType(Class<?> type) {
        switch (type.getName()) {
            case "String" -> {
                return "VARCHAR(255)";
            }
            case "Integer", "int" -> {
                return "INT";
            }
            case "Long", "long" -> {
                return "BIGINT";
            }
            case "Boolean", "boolean" -> {
                return "BOOLEAN";
            }
            case "Double", "double" -> {
                return "DOUBLE";
            }
            case "Float", "float" -> {
                return "FLOAT";
            }
            case "Short", "short" -> {
                return "SMALLINT";
            }
            case "Byte", "byte" -> {
                return "TINYINT";
            }
            case "Character", "char" -> {
                return "CHAR(1)";
            }
            case "UUID" -> {
                return "VARCHAR(36)";
            }
            case "Timestamp" -> {
                return "TIMESTAMP";
            }
            case "Date" -> {
                return "DATE";
            }
            case "Time" -> {
                return "TIME";
            }
            case "BigDecimal" -> {
                return "DECIMAL";
            }
            case "Blob" -> {
                return "BLOB";
            }
            case "Clob" -> {
                return "CLOB";
            }
            case "Array" -> {
                return "ARRAY";
            }
            case "Ref" -> {
                return "REF";
            }
            case "NClob" -> {
                return "NCLOB";
            }
            case "RowId" -> {
                return "ROWID";
            }
            case "SQLXML" -> {
                return "SQLXML";
            }
            default -> {
                return "VARCHAR(255)";
            }
        }
    }

    default CompletableFuture<Void> save(V value) {
        return CompletableFuture.runAsync(() -> {
            Map<String, String> insertMap = getInsertIntoTables().get();

            String insert = insertMap.get(table());
            if (insert == null) {
                throw new IllegalStateException("Could not find insert statement for table " + table());
            }

            Object id = IdUtils.getId(value.getClass(), value);

            execute(insert, preparedStatement -> {
                setStatement(preparedStatement, 1, id);
                int i = 2;
                for (Field field : getSerializableFields(value.getClass())) {
                    if (field.getType().isAnnotationPresent(StorageSerialized.class)) {
                        continue;
                    }
                    try {
                        field.setAccessible(true);
                        setStatement(preparedStatement, i, field.get(value));
                        i++;
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    }
                }
            });


            insertMap.forEach((parent, statement) -> {
                if (parent.equals(table())) {
                    return;
                }

                execute(statement, preparedStatement -> {
                    setStatement(preparedStatement, 1, id);
                    int i = 2;
                    for (Field field : getSerializableFields(value.getClass())) {
                        if (!field.getType().isAnnotationPresent(StorageSerialized.class)) {
                            continue;
                        }
                        try {
                            field.setAccessible(true);
                            setStatement(preparedStatement, i, field.get(value));
                            i++;
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        }
                    }
                });
            });
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
