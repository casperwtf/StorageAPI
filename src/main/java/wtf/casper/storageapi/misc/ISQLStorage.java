package wtf.casper.storageapi.misc;

import com.zaxxer.hikari.HikariDataSource;
import lombok.SneakyThrows;
import wtf.casper.storageapi.StatelessFieldStorage;
import wtf.casper.storageapi.id.StorageSerialized;
import wtf.casper.storageapi.id.Transient;
import wtf.casper.storageapi.id.utils.IdUtils;
import wtf.casper.storageapi.utils.Constants;
import wtf.casper.storageapi.utils.ReflectionUtil;
import wtf.casper.storageapi.utils.UnsafeConsumer;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

public interface ISQLStorage<K, V> extends StatelessFieldStorage<K, V>, ConstructableValue<K, V> {

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

    default boolean requiresSubTable(String parent, Field field) {
        return Iterable.class.isAssignableFrom(field.getType()) || Map.class.isAssignableFrom(field.getType()) || field.getType().isArray();
    }

    default void createSubTable(String parent, Field field) {
        if (Iterable.class.isAssignableFrom(field.getType())) {
            Class<?> genericType = ReflectionUtil.getGenericType(field, 0);
            if (genericType == null) {
                throw new IllegalStateException("Could not find generic type for " + field.getType().getName());
            }
            this.execute("CREATE TABLE IF NOT EXISTS " + getTable() + "_" + parent + "_" + field.getName() + " (" + createSubTables(field, null, field.getName() + ".") + ");");
        } else if (field.getType().isArray()) {
            Class<?> componentType = field.getType().getComponentType();
            if (componentType == null) {
                throw new IllegalStateException("Could not find component type for " + field.getType().getName());
            }
            this.execute("CREATE TABLE IF NOT EXISTS " + getTable() + "_" + parent + "_" + field.getName() + " (" + createSubTables(field, null, field.getName() + ".") + ");");
        } else if (Map.class.isAssignableFrom(field.getType())) {
            Class<?> genericType = ReflectionUtil.getGenericType(field, 1);
            if (genericType == null) {
                throw new IllegalStateException("Could not find generic type for " + field.getType().getName());
            }
            this.execute("CREATE TABLE IF NOT EXISTS " + getTable() + "_" + parent + "_" + field.getName() + " (" + createSubTables(field, null, field.getName() + ".") + ");");
        }
    }

    default List<Field> getFields(Class<?> clazz) {
        return Arrays.stream(clazz.getDeclaredFields())
                .filter(field -> !field.isAnnotationPresent(Transient.class))
                .filter(field -> !Modifier.isTransient(field.getModifiers()))
                .filter(field -> !Modifier.isStatic(field.getModifiers()))
                .toList();
    }

    /**
     * Will scan the class for fields and add them to the database if they don't exist
     */
    default void scanForMissingColumns() {
        List<Field> fields = getFields(value());

        for (Field declaredField : fields) {
            declaredField.setAccessible(true);
            Map<String, String> columns = new HashMap<>();

            if (Iterable.class.isAssignableFrom(declaredField.getType())) {
                Class<?> genericType = ReflectionUtil.getGenericType(declaredField, 0);
                if (genericType == null) {
                    throw new IllegalStateException("Could not find generic type for " + declaredField.getType().getName());
                }
                columns.putAll(scanForMissingColumnsSub(declaredField, null, declaredField.getName() + "."));
            } else if (declaredField.getType().isArray()) {
                Class<?> componentType = declaredField.getType().getComponentType();
                if (componentType == null) {
                    throw new IllegalStateException("Could not find component type for " + declaredField.getType().getName());
                }
                columns.putAll(scanForMissingColumnsSub(declaredField, null, declaredField.getName() + "."));
            } else if (declaredField.getType().isAnnotationPresent(StorageSerialized.class)) {
                columns.putAll(scanForMissingColumnsSub(declaredField, null, declaredField.getName() + "."));
            } else {
                columns.put(declaredField.getName(), getType(declaredField.getType()));
            }

            this.query("SELECT * FROM " + getTable() + " LIMIT 1;", resultSet -> {
                columns.forEach((s, s2) -> {
                    try {
                        if (resultSet.findColumn(s) == 0) {
                            this.addColumn(s, s2);
                        }
                    } catch (SQLException e) {
                        this.addColumn(s, s2);
                    }
                });
                resultSet.close();
            });
        }
    }

    default Map<String, String> scanForMissingColumnsSub(Field field, Class<?> clazz, String parentPrefix) {
        if (field == null && clazz == null) {
            throw new IllegalStateException("Field and clazz cannot both be null");
        }

        final Class<?> type = field == null ? clazz : field.getType();

        if (Iterable.class.isAssignableFrom(type)) {
            if (field == null) {
                throw new IllegalStateException("Subtable cannot generate subtable from null field");
            }
            Class<?> genericType = ReflectionUtil.getGenericType(field, 0);
            if (genericType == null) {
                throw new IllegalStateException("Could not find generic type for " + type.getName());
            }
            return scanForMissingColumnsSub(null, genericType, parentPrefix + field.getName() + ".");
        } else if (type.isArray()) {
            Class<?> componentType = type.getComponentType();
            if (componentType == null) {
                throw new IllegalStateException("Could not find component type for " + type.getName());
            }
            if (field == null) {
                throw new IllegalStateException("Subtable cannot generate subtable from null field");
            }

            return scanForMissingColumnsSub(null, componentType, parentPrefix + field.getName() + ".");
        }

        List<Field> fields = getFields(type);

        if (fields.isEmpty()) {
            throw new IllegalStateException("Could not find any fields for " + type.getName());
        }

        Map<String, String> columns = new HashMap<>();
        for (Field declaredField : fields) {
            declaredField.setAccessible(true);
            final String name = declaredField.getName();
            String type1 = this.getType(declaredField.getType());

            if (declaredField.getType().isAnnotationPresent(StorageSerialized.class)) {
                columns.putAll(scanForMissingColumnsSub(declaredField, null, parentPrefix + name + "."));
            } else {
                columns.put(parentPrefix + name, type1);
            }
        }

        return columns;
    }

    /**
     * Generate an SQL Script to create the table based on the class
     */
    default String createTableFromObject() {
        final StringBuilder builder = new StringBuilder();

        List<Field> fields = getFields(value());

        if (fields.size() == 0) {
            throw new IllegalStateException("Could not find any fields for " + value().getName());
        }

        builder.append("CREATE TABLE IF NOT EXISTS ").append(getTable()).append(" (");

        String idName = IdUtils.getIdName(value());

        int index = 0;
        for (Field declaredField : fields) {
            declaredField.setAccessible(true);
            final String name = declaredField.getName();
            String type = this.getType(declaredField.getType());
            if (Iterable.class.isAssignableFrom(declaredField.getType())) {
                Class<?> genericType = ReflectionUtil.getGenericType(declaredField, 0);
                if (genericType == null) {
                    throw new IllegalStateException("Could not find generic type for " + declaredField.getType().getName());
                }

                builder.append(createSubTables(declaredField, null, declaredField.getName() + "."));
            } else if (declaredField.getType().isArray()) {
                Class<?> componentType = declaredField.getType().getComponentType();
                if (componentType == null) {
                    throw new IllegalStateException("Could not find component type for " + declaredField.getType().getName());
                }

                builder.append(createSubTables(declaredField, null, declaredField.getName() + "."));
            } else if (declaredField.getType().isAnnotationPresent(StorageSerialized.class)) {
                builder.append(createSubTables(declaredField, null, declaredField.getName() + "."));
            } else {
                builder.append("`").append(name).append("`").append(" ").append(type);
                if (name.equals(idName)) {
                    builder.append(" PRIMARY KEY");
                }
            }

            index++;
            if (index != fields.size()) {
                builder.append(", ");
            }
        }
        builder.append(");");
        return builder.toString();
    }

    default String createSubTables(@Nullable Field field, @Nullable Class<?> directClazz, String parentPrefix) {
        if (field == null && directClazz == null) {
            throw new IllegalStateException("Field and directClazz cannot both be null");
        }

        final Class<?> clazz = field == null ? directClazz : field.getType();

        if (Iterable.class.isAssignableFrom(clazz)) {
            if (field == null) {
                throw new IllegalStateException("Subtable cannot generate subtable from null field");
            }
            Class<?> genericType = ReflectionUtil.getGenericType(field, 0);
            if (genericType == null) {
                throw new IllegalStateException("Could not find generic type for " + clazz.getName());
            }
            return createSubTables(null, genericType, parentPrefix + field.getName() + ".");
        } else if (clazz.isArray()) {
            Class<?> componentType = clazz.getComponentType();
            if (componentType == null) {
                throw new IllegalStateException("Could not find component type for " + clazz.getName());
            }
            if (field == null) {
                throw new IllegalStateException("Subtable cannot generate subtable from null field");
            }

            return createSubTables(null, componentType, parentPrefix + field.getName() + ".");
        }

        List<Field> fields = getFields(clazz);

        if (fields.isEmpty()) {
            throw new IllegalStateException("Could not find any fields for " + clazz.getName());
        }

        StringBuilder builder = new StringBuilder();
        int index = 0;
        for (Field declaredField : fields) {
            declaredField.setAccessible(true);
            final String name = declaredField.getName();
            String type = this.getType(declaredField.getType());

            if (declaredField.getType().isAnnotationPresent(StorageSerialized.class)) {
                builder.append(createSubTables(declaredField, null, parentPrefix + name + "."));
            } else {
                builder.append("`").append(parentPrefix).append(name).append("`").append(" ").append(type);
            }

            index++;
            if (index != fields.size()) {
                builder.append(", ");
            }

        }

        return builder.toString();
    }

    /**
     * This takes an SQL Result Set and parses it into an object
     */
    @SneakyThrows
    default V construct(final ResultSet resultSet) {
        final V value = constructValue();
        List<Field> fields = getFields(value());

        for (Field declaredField : fields) {
            declaredField.setAccessible(true);
            if (declaredField.isAnnotationPresent(StorageSerialized.class)) {
                final String name = declaredField.getName();
                final Object object = constructSubObject(name, declaredField.getType(), resultSet);
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

    default Object constructSubObject(String name, Class<?> clazz, ResultSet resultSet) {
        Map<String, Object> subResultSet = new HashMap<>();
        List<Field> fields = getFields(clazz);
        try {
            for (Field field : fields) {
                field.setAccessible(true);
                if (field.getType().isAnnotationPresent(StorageSerialized.class)) {
                    Object object = constructSubObject(name + "." + field.getName(), field.getType(), resultSet);
                    subResultSet.put(field.getName(), object);
                    continue;
                }

                Object object = resultSet.getObject(name + "." + field.getName());
                subResultSet.put(field.getName(), object);
            }

            Object o = Constants.OBJENESIS_STD.newInstance(clazz);
            for (Map.Entry<String, Object> entry : subResultSet.entrySet()) {
                ReflectionUtil.setPrivateField(o, entry.getKey(), entry.getValue());
            }
            return o;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    default String getUpdateValues() {
        final StringBuilder builder = new StringBuilder();
        int i = 0;

        List<Field> fields = getFields(value());

        for (final Field field : fields) {
            field.setAccessible(true);
            if (field.getType().isAnnotationPresent(StorageSerialized.class)) {
                builder.append(getUpdateValuesSub(field, null, field.getName() + "."));
            } else {
                builder.append("`").append(field.getName()).append("` = excluded.`").append(field.getName()).append("`");
            }

            if (i != fields.size() - 1) {
                builder.append(", ");
            }

            i++;
        }

        return builder.toString();
    }

    default String getUpdateValuesSub(Field field, Class<?> clazz, String parentPrefix) {
        if (field == null && clazz == null) {
            throw new IllegalStateException("Field and clazz cannot both be null");
        }

        final Class<?> type = field == null ? clazz : field.getType();

        if (Iterable.class.isAssignableFrom(type)) {
            if (field == null) {
                throw new IllegalStateException("Subtable cannot generate subtable from null field");
            }
            Class<?> genericType = ReflectionUtil.getGenericType(field, 0);
            if (genericType == null) {
                throw new IllegalStateException("Could not find generic type for " + type.getName());
            }
            return getUpdateValuesSub(null, genericType, parentPrefix + field.getName() + ".");
        } else if (type.isArray()) {
            Class<?> componentType = type.getComponentType();
            if (componentType == null) {
                throw new IllegalStateException("Could not find component type for " + type.getName());
            }
            if (field == null) {
                throw new IllegalStateException("Subtable cannot generate subtable from null field");
            }

            return getUpdateValuesSub(null, componentType, parentPrefix + field.getName() + ".");
        }

        List<Field> fields = getFields(type);

        if (fields.isEmpty()) {
            throw new IllegalStateException("Could not find any fields for " + type.getName());
        }

        StringBuilder builder = new StringBuilder();
        for (Field declaredField : fields) {
            declaredField.setAccessible(true);
            final String name = declaredField.getName();
            String type1 = this.getType(declaredField.getType());

            if (declaredField.getType().isAnnotationPresent(StorageSerialized.class)) {
                builder.append(getUpdateValuesSub(declaredField, null, parentPrefix + name + "."));
            } else {
                builder.append("`").append(parentPrefix).append(name).append("`").append(" ").append(type1);
            }
        }

        return builder.toString();
    }

    /**
     * Generates an SQL String for the columns associated with a value class.
     */
    default String getColumns() {
        final StringBuilder builder = new StringBuilder();

        List<Field> fields = getFields(value());

        for (final Field field : fields) {
            field.setAccessible(true);
            if (Iterable.class.isAssignableFrom(field.getType())) {
                Class<?> genericType = ReflectionUtil.getGenericType(field, 0);
                if (genericType == null) {
                    throw new IllegalStateException("Could not find generic type for " + field.getType().getName());
                }
                builder.append(getSubColumns(field, null, field.getName() + "."));
            } else if (field.getType().isArray()) {
                Class<?> componentType = field.getType().getComponentType();
                if (componentType == null) {
                    throw new IllegalStateException("Could not find component type for " + field.getType().getName());
                }
                builder.append(getSubColumns(field, null, field.getName() + "."));
            } else if (field.getType().isAnnotationPresent(StorageSerialized.class)) {
                builder.append(getSubColumns(field, null, field.getName() + "."));
            } else {
                builder.append("`").append(field.getName()).append("`").append(",");
            }
        }

        return builder.substring(0, builder.length() - 1);
    }

    default String getSubColumns(@Nullable Field field, @Nullable Class<?> directClazz, String parentPrefix) {
        if (field == null && directClazz == null) {
            throw new IllegalStateException("Field and directClazz cannot both be null");
        }

        final Class<?> clazz = field == null ? directClazz : field.getType();

        if (Iterable.class.isAssignableFrom(clazz)) {
            if (field == null) {
                throw new IllegalStateException("Subtable cannot generate subtable from null field");
            }
            Class<?> genericType = ReflectionUtil.getGenericType(field, 0);
            if (genericType == null) {
                throw new IllegalStateException("Could not find generic type for " + clazz.getName());
            }
            return getSubColumns(null, genericType, parentPrefix + field.getName() + ".");
        } else if (clazz.isArray()) {
            Class<?> componentType = clazz.getComponentType();
            if (componentType == null) {
                throw new IllegalStateException("Could not find component type for " + clazz.getName());
            }
            if (field == null) {
                throw new IllegalStateException("Subtable cannot generate subtable from null field");
            }

            return getSubColumns(null, componentType, parentPrefix + field.getName() + ".");
        }

        List<Field> fields = getFields(clazz);

        if (fields.isEmpty()) {
            throw new IllegalStateException("Could not find any fields for " + clazz.getName());
        }

        StringBuilder builder = new StringBuilder();
        for (Field declaredField : fields) {
            declaredField.setAccessible(true);
            final String name = declaredField.getName();
            String type = this.getType(declaredField.getType());

            if (declaredField.getType().isAnnotationPresent(StorageSerialized.class)) {
                builder.append(getSubColumns(declaredField, null, parentPrefix + name + "."));
            } else {
                builder.append("`").append(parentPrefix).append(name).append("`").append(" ").append(type);
            }
        }

        return builder.toString();
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


    /**
     * Generates an SQL String for inserting a value into the database.
     */
    default String getValues(Object value, Class<?> clazz) {
        final StringBuilder builder = new StringBuilder();
        int i = 0;

        List<Field> fields = getFields(clazz);

        for (final Field field : fields) {
            field.setAccessible(true);
            Object privateField = ReflectionUtil.getPrivateField(value, field.getName());
            if (field.getType().isAnnotationPresent(StorageSerialized.class)) {
                try {
                    Object o = field.get(value);
                    builder.append(getValues(o, field.getType())).append(", ");
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
                continue;
            }

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

            if (i != fields.size() - 1) {
                builder.append(", ");
            }
            i++;
        }

        String string = builder.toString();
        if (string.endsWith(", ")) {
            string = string.substring(0, string.length() - 2);
        }
        return string;
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
