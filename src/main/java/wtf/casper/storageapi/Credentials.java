package wtf.casper.storageapi;

import lombok.*;
import org.jetbrains.annotations.Nullable;

@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
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
