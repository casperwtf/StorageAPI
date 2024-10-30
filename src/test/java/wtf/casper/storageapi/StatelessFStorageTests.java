package wtf.casper.storageapi;

import lombok.extern.java.Log;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import wtf.casper.storageapi.impl.direct.statelessfstorage.DirectStatelessMariaDBFStorage;
import wtf.casper.storageapi.impl.direct.statelessfstorage.DirectStatelessMongoFStorage;

import java.io.File;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Log
public class StatelessFStorageTests {

    @BeforeAll
    public static void setup() {
        InputStream stream = StatelessFStorageTests.class.getClassLoader().getResourceAsStream("storage.properties");
        File file = new File("."+File.separator+"storage.properties");
        if (file.exists()) {
            stream = file.toURI().toASCIIString().contains("jar") ? StatelessFStorageTests.class.getClassLoader().getResourceAsStream("storage.properties") : null;
        }

        if (stream == null) {
            log.severe("Could not find storage.properties file!");
            return;
        }

        Properties properties = new Properties();
        try {
            properties.load(stream);
        } catch (Exception e) {
            e.printStackTrace();
        }

        init(properties);
    }

    public static void init(Properties properties) {
        StorageType type = StorageType.valueOf((String) properties.get("storage.type"));
        credentials = Credentials.of(
                type,
                (String) properties.get("storage.host"),
                (String) properties.get("storage.username"),
                (String) properties.get("storage.password"),
                (String) properties.get("storage.database"),
                (String) properties.get("storage.collection"),
                (String) properties.get("storage.table"),
                (String) properties.get("storage.uri"),
                Integer.parseInt((String) properties.get("storage.port"))
        );


        switch (type) {
            case MONGODB -> storage = new DirectStatelessMongoFStorage<>(UUID.class, TestObject.class, credentials, TestObject::new);
            case SQLITE -> throw new UnsupportedOperationException("SQLite is not supported yet!");
            case SQL -> throw new UnsupportedOperationException("SQL is not supported yet!");
            case MARIADB -> storage = new DirectStatelessMariaDBFStorage<>(UUID.class, TestObject.class, credentials, TestObject::new);
            case JSON -> throw new UnsupportedOperationException("JSON is not supported yet!");
            default -> throw new IllegalStateException("Unexpected value: " + type);
        }

        storage.deleteAll().join();
        storage.saveAll(initialData).join();
        storage.write().join();
    }

    private static Credentials credentials;
    private static StatelessFieldStorage<UUID, TestObject> storage;

    private static final List<TestObject> initialData = List.of(
            new TestObject(
                    UUID.fromString("00000000-0000-0000-0000-000000000002"), "Mike", 25,
                    new TestObjectData("5678 Elm Avenue", "Fake Employer C", "fakemikec@gmail.com", "987-654-3210",
                            15, new TestObjectBalance(150, "USD")
                    )
            ),
            new TestObject(
                    UUID.fromString("00000000-0000-0000-0000-000000000003"), "Emily", 22,
                    new TestObjectData("7890 Oak Street", "Fake Employer D", "fakeemilyd@gmail.com", "555-555-5555",
                            17, new TestObjectBalance(50, "USD")
                    )
            ),
            new TestObject(
                    UUID.fromString("00000000-0000-0000-0000-000000000004"), "Michael", 30,
                    new TestObjectData("1111 Maple Avenue", "Fake Employer E", "fakemichaele@gmail.com", "111-222-3333",
                            19, new TestObjectBalance(300, "USD")
                    )
            ),
            new TestObject(
                    UUID.fromString("00000000-0000-0000-0000-000000000005"), "Sarah", 27,
                    new TestObjectData("2222 Pine Street", "Fake Employer F", "fakesarahf@gmail.com", "444-555-6666",
                            18, new TestObjectBalance(75, "USD")
                    )
            ),
            new TestObject(
                    UUID.fromString("00000000-0000-0000-0000-000000000006"), "David", 32,
                    new TestObjectData("3333 Cedar Avenue", "Fake Employer G", "fakedavidg@gmail.com", "777-888-9999",
                            20, new TestObjectBalance(250, "USD")
                    )
            ),
            new TestObject(
                    UUID.fromString("00000000-0000-0000-0000-000000000007"), "Olivia", 21,
                    new TestObjectData("4444 Birch Street", "Fake Employer H", "fakeoliviah@gmail.com", "000-111-2222",
                            21, new TestObjectBalance(125, "USD")
                    )
            ),
            new TestObject(
                    UUID.fromString("00000000-0000-0000-0000-000000000008"), "Daniel", 29,
                    new TestObjectData("5555 Willow Avenue", "Fake Employer I", "fakedanieli@gmail.com", "333-444-5555",
                            18, new TestObjectBalance(180, "USD")
                    )
            ),
            new TestObject(
                    UUID.fromString("00000000-0000-0000-0000-000000000009"), "Sophia", 26,
                    new TestObjectData("6666 Elm Avenue", "Fake Employer J", "fakesophiaj@gmail.com", "666-777-8888",
                            16, new TestObjectBalance(90, "USD")
                    )
            ),
            new TestObject(
                    UUID.fromString("00000000-0000-0000-0000-000000000010"), "James", 28,
                    new TestObjectData("7777 Oak Street", "Fake Employer K", "fakejamesk@gmail.com", "999-000-1111",
                            18, new TestObjectBalance(160, "USD")
                    )
            ),
            new TestObject(
                    UUID.fromString("00000000-0000-0000-0000-000000000011"), "Emma", 23,
                    new TestObjectData("8888 Maple Avenue", "Fake Employer L", "fakeemmal@gmail.com", "222-333-4444",
                            10, new TestObjectBalance(220, "USD")
                    )
            ),
            new TestObject(
                    UUID.fromString("00000000-0000-0000-0000-000000000012"), "Benjamin", 31,
                    new TestObjectData("9999 Pine Street", "Fake Employer M", "fakebenjaminm@gmail.com", "555-666-7777",
                            55, new TestObjectBalance(110, "USD")
                    )
            ),
            new TestObject(
                    UUID.fromString("00000000-0000-0000-0000-000000000013"), "Ava", 24,
                    new TestObjectData("1111 Cedar Avenue", "Fake Employer N", "fakeavan@gmail.com", "888-999-0000",
                            66666, new TestObjectBalance(270, "USD")
                    )
            ),
            new TestObject(
                    UUID.fromString("00000000-0000-0000-0000-000000000014"), "Ethan", 33,
                    new TestObjectData("2222 Birch Street", "Fake Employer O", "fakeethano@gmail.com", "111-222-3333",
                            888, new TestObjectBalance(80, "USD")
                    )
            ),
            new TestObject(
                    UUID.fromString("00000000-0000-0000-0000-000000000015"), "Mia", 20,
                    new TestObjectData("3333 Willow Avenue", "Fake Employer P", "fakemiap@gmail.com", "444-555-6666",
                            0, new TestObjectBalance(140, "USD")
                    )
            ),
            new TestObject(
                    UUID.fromString("00000000-0000-0000-0000-000000000000"), "John", 18,
                    new TestObjectData("1234 Fake Street", "Fake Employer A", "fakejohna@gmail.com", "123-456-7890",
                            11, new TestObjectBalance(100, "USD")
                    )
            ),
            new TestObject(
                    UUID.fromString("00000000-0000-0000-0000-000000000001"), "Jane", 19,
                    new TestObjectData("1234 Fake Street", "Fake Employer B", "fakejanea@gmail.com", "123-456-7890",
                            19, new TestObjectBalance(200, "GBP")
                    )
            )
    );

    
    @Test
    public void testTotalData() {
        assertEquals(initialData.size(), storage.allValues().join().size());
    }

    @Test
    public void testStartsWith() {
        Collection<TestObject> street = storage.get(
                Filter.of("data.address", "1", FilterType.STARTS_WITH)
        ).join();
        assertEquals(4, street.size());
    }

    @Test
    public void testEndsWith() {

        Collection<TestObject> street = storage.get(
                Filter.of("name", "a", FilterType.ENDS_WITH)
        ).join();
        assertEquals(5, street.size());

        Collection<TestObject> phone = storage.get(
                Filter.of("data.phone", "0", FilterType.ENDS_WITH)
        ).join();
        assertEquals(4, phone.size());
    }

    @Test
    public void testGreaterThan() {
        Collection<TestObject> street = storage.get(
                Filter.of("age", 20, FilterType.GREATER_THAN)
        ).join();
        assertEquals(13, street.size());
    }

    @Test
    public void testLessThan() {
        Collection<TestObject> street = storage.get(
                Filter.of("age", 20, FilterType.LESS_THAN)
        ).join();
        assertEquals(2, street.size());
    }

    @Test
    public void testContains() {
        Collection<TestObject> street = storage.get(
                Filter.of("data.address", "Street", FilterType.CONTAINS),
                Filter.of("age", 18, FilterType.EQUALS)
        ).join();
        assertEquals(1, street.size());

        Collection<TestObject> street1 = storage.get(
                Filter.of("data.address", "Street", FilterType.CONTAINS)
        ).join();
        assertEquals(8, street1.size());

        CompletableFuture<Collection<TestObject>> allStreets = storage.get(
                Filter.of("data.address", "Street", FilterType.CONTAINS),
                Filter.of("data.address", "Avenue", FilterType.CONTAINS, SortingType.NONE, Filter.Type.OR),
                Filter.of("data.address", "Random Garbage", FilterType.CONTAINS, SortingType.NONE, Filter.Type.OR)

        );
        assertEquals(16, allStreets.join().size());
    }

    @Test
    public void testEquals() {
        Collection<TestObject> usd = storage.get(
                Filter.of("data.balance.currency", "USD", FilterType.EQUALS)
        ).join();
        assertEquals(15, usd.size());
    }

    @Test
    public void testNotEquals() {
        Collection<TestObject> usd = storage.get(
                Filter.of("data.balance.currency", "USD", FilterType.NOT_EQUALS)
        ).join();
        assertEquals(1, usd.size());
    }

    @Test
    public void testNotContains() {
        Collection<TestObject> street = storage.get(
                Filter.of("data.address", "Street", FilterType.NOT_CONTAINS)
        ).join();
        assertEquals(8, street.size());
    }

    @Test
    public void testNotStartsWith() {
        Collection<TestObject> street = storage.get(
                Filter.of("data.address", "1", FilterType.NOT_STARTS_WITH)
        ).join();
        assertEquals(12, street.size());
    }

    @Test
    public void testNotEndsWith() {
        Collection<TestObject> street = storage.get(
                Filter.of("name", "a", FilterType.NOT_ENDS_WITH)
        ).join();
        assertEquals(11, street.size());
    }

    @Test
    public void testLessThanOrEqualTo() {
        Collection<TestObject> street = storage.get(
                Filter.of("age", 20, FilterType.LESS_THAN_OR_EQUAL_TO)
        ).join();
        assertEquals(3, street.size());
    }

    @Test
    public void testGreaterThanOrEqualTo() {
        Collection<TestObject> street = storage.get(
                Filter.of("age", 20, FilterType.GREATER_THAN_OR_EQUAL_TO)
        ).join();
        assertEquals(14, street.size());
    }

    @Test
    public void testNotLessThan() {
        Collection<TestObject> street = storage.get(
                Filter.of("age", 20, FilterType.NOT_LESS_THAN)
        ).join();
        assertEquals(14, street.size());
    }

    @Test
    public void testNotGreaterThan() {
        Collection<TestObject> street = storage.get(
                Filter.of("age", 20, FilterType.NOT_GREATER_THAN)
        ).join();
        assertEquals(3, street.size());
    }

    @Test
    public void testNotLessThanOrEqualTo() {
        Collection<TestObject> street = storage.get(
                Filter.of("age", 20, FilterType.NOT_LESS_THAN_OR_EQUAL_TO)
        ).join();
        assertEquals(13, street.size());
    }

    @Test
    public void testNotGreaterThanOrEqualTo() {
        Collection<TestObject> street = storage.get(
                Filter.of("age", 20, FilterType.NOT_GREATER_THAN_OR_EQUAL_TO)
        ).join();
        assertEquals(2, street.size());
    }

    @Test
    public void testAnd() {
        Collection<TestObject> street = storage.get(
                Filter.of("age", 20, FilterType.GREATER_THAN),
                Filter.of("data.address", "Street", FilterType.CONTAINS)
        ).join();
        assertEquals(6, street.size());
    }

    @Test
    public void testLimit() {
        Collection<TestObject> street = storage.get(10,
                Filter.of("age", 20, FilterType.GREATER_THAN_OR_EQUAL_TO)
        ).join();
        assertEquals(10, street.size());
    }


}
