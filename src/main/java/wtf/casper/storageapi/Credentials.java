package wtf.casper.storageapi;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.Nullable;

@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class Credentials {

    private final StorageType type;

    @Nullable
    private String host;
    @Nullable
    private String username;
    @Nullable
    private String password;
    @Nullable
    private String database;
    @Nullable
    private String collection;
    @Nullable
    private String table;
    @Nullable
    private String uri;
    @Nullable
    private Integer port;

    public static Credentials of(final StorageType type, @Nullable final String host, @Nullable final String username, @Nullable final String password, @Nullable final String database, @Nullable String collection, @Nullable String table, @Nullable final String uri, final int port) {
        return new Credentials(type, host, username, password, database, collection, table, uri, port);
    }

    public StorageType getType(StorageType defaultValue) {
        return type == null ? defaultValue : type;
    }

    public String getHost(String defaultValue) {
        return host == null ? defaultValue : host;
    }

    public String getUsername(String defaultValue) {
        return username == null ? defaultValue : username;
    }

    public String getPassword(String defaultValue) {
        return password == null ? defaultValue : password;
    }

    public String getDatabase(String defaultValue) {
        return database == null ? defaultValue : database;
    }

    public String getCollection(String defaultValue) {
        return collection == null ? defaultValue : collection;
    }

    public String getTable(String defaultValue) {
        return table == null ? defaultValue : table;
    }

    public String getUri(String defaultValue) {
        return uri == null ? defaultValue : uri;
    }

    public int getPort(int defaultValue) {
        return port == null ? defaultValue : port;
    }
}
