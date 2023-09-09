package wtf.casper.storageapi.misc;

import com.zaxxer.hikari.HikariDataSource;
import wtf.casper.storageapi.StatelessKVStorage;
import wtf.casper.storageapi.id.utils.IdUtils;
import wtf.casper.storageapi.utils.Constants;
import wtf.casper.storageapi.utils.UnsafeConsumer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

public interface ISQLKVStorage<K, V> extends StatelessKVStorage<K, V>, ConstructableValue<K, V> {

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

    default void createTable() {
        String idName = IdUtils.getIdName(value());
        boolean isUUID = UUID.class.isAssignableFrom(IdUtils.getIdClass(value()));
        String idType = isUUID ? "VARCHAR(36) NOT NULL" : "VARCHAR(255) NOT NULL";
        idType = idName + " " + idType + " PRIMARY KEY";

        execute("CREATE TABLE IF NOT EXISTS " + table() + " (" + idType + ", json LONGTEXT NOT NULL);");
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

    default CompletableFuture<Void> remove(V value) {
        return CompletableFuture.runAsync(() -> {
            Object id = IdUtils.getId(value());
            if (id == null) {
                logger().warning("Could not find id field for " + value().getSimpleName());
                return;
            }

            String idName = IdUtils.getIdName(value());
            executeUpdate("DELETE FROM " + table() + " WHERE `" + idName + "` = ?;", statement -> {
                statement.setString(1, id.toString());
            });
        });
    }

    default CompletableFuture<V> get(K key) {
        return CompletableFuture.supplyAsync(() -> {
            String idName = IdUtils.getIdName(value());

            AtomicReference<V> value = new AtomicReference<>();

            query("SELECT * FROM " + table() + " WHERE `" + idName + "` = ?;", statement -> {
                statement.setString(1, key.toString());
            }, resultSet -> {
                try {
                    if (resultSet.next()) {
                        value.set(Constants.getGson().fromJson(resultSet.getString("json"), value()));
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }

                resultSet.close();
            }).join();

            return value.get();
        });
    }
}
