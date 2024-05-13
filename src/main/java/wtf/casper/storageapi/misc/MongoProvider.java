package wtf.casper.storageapi.misc;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoClientURI;
import com.mongodb.client.ClientSession;
import org.bson.UuidRepresentation;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.pojo.PojoCodecProvider;

import java.util.HashMap;
import java.util.Map;

public class MongoProvider {
    private static final Map<String, MongoClient> clients = new HashMap<>();
    private static String defaultConnection;

    public static MongoClient getClient(String uri) {
        if (clients.containsKey(uri)) {
            MongoClient client = clients.get(uri);
            try {
                // Test connection
                client.getAddress();
                client.getConnectPoint();
                ClientSession session = client.startSession();
                session.close();
                return client;
            } catch (Exception e) {
                client.close();
                clients.remove(uri);
            }
        }

        MongoClientURI mongoClientURI = new MongoClientURI(uri, MongoClientOptions.builder()
                .uuidRepresentation(UuidRepresentation.JAVA_LEGACY)
                .codecRegistry(CodecRegistries.fromRegistries(
                        MongoClientSettings.getDefaultCodecRegistry(),
                        CodecRegistries.fromProviders(PojoCodecProvider.builder().automatic(true).build())
                ))
        );
        MongoClient client = new MongoClient(mongoClientURI);
        clients.put(uri, client);
        return client;
    }

    public static void setDefaultConnection(String uri) {
        defaultConnection = uri;
    }

    public static MongoClient getClient() {
        if (defaultConnection == null) {
            throw new IllegalStateException("Default connection not set");
        }
        if (!clients.containsKey(defaultConnection)) {
            throw new IllegalStateException("Default connection not initialized");
        }
        return getClient(defaultConnection);
    }

    public static void closeClient(String uri) {
        if (clients.containsKey(uri)) {
            clients.get(uri).close();
            clients.remove(uri);
        }
    }

    public static void closeClient() {
        if (defaultConnection == null) {
            throw new IllegalStateException("Default connection not set");
        }
        if (!clients.containsKey(defaultConnection)) {
            throw new IllegalStateException("Default connection not initialized");
        }
        closeClient(defaultConnection);
        defaultConnection = null;
    }
}
