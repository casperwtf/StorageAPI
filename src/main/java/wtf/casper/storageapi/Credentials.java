package wtf.casper.storageapi;

import dev.dejvokep.boostedyaml.YamlDocument;
import dev.dejvokep.boostedyaml.block.implementation.Section;
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

    @Nullable private String host;
    @Nullable private String username;
    @Nullable private String password;
    @Nullable private String database;
    @Nullable private String collection;
    @Nullable private String table;
    @Nullable private String uri;
    @Nullable private Integer port;

    public static Credentials of(final StorageType type, @Nullable final String host, @Nullable final String username, @Nullable final String password, @Nullable final String database, @Nullable String collection, @Nullable String table, @Nullable final String uri, final int port) {
        return new Credentials(type, host, username, password, database, collection, table, uri, port);
    }

    /**
     * @param config The YAML config file
     * @param path   The path to the credentials section
     *               <p>
     *               Note: If the port fails to parse, it will default to 3306
     *               </p>
     */
    public static Credentials from(final YamlDocument config, String path) {
        final Section section = config.getSection(path);

        return of(StorageType.valueOf(section.getString("type", "JSON").toUpperCase()),
                section.getString("host", null),
                section.getString("username", null),
                section.getString("password", null),
                section.getString("database", null),
                section.getString("collection", null),
                section.getString("table", null),
                section.getString("uri", null),
                section.getOptionalString("port").map(s -> {
                    try {
                        return Integer.parseInt(s);
                    } catch (NumberFormatException e) {
                        return 3306;
                    }
                }).orElse(section.getInt("port", 3306))
        );
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
