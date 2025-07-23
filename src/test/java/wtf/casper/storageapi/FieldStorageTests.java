package wtf.casper.storageapi;

import lombok.extern.java.Log;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import wtf.casper.storageapi.impl.direct.fstorage.DirectMariaDBFStorage;
import wtf.casper.storageapi.impl.direct.fstorage.DirectMongoFStorage;

import java.io.File;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Log
public class FieldStorageTests {

    @BeforeAll
    public static void setup() {
        InputStream stream = FieldStorageTests.class.getClassLoader().getResourceAsStream("storage.properties");
        File file = new File("."+File.separator+"storage.properties");
        if (file.exists()) {
            stream = file.toURI().toASCIIString().contains("jar") ? FieldStorageTests.class.getClassLoader().getResourceAsStream("storage.properties") : null;
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
        credentials = Credentials.builder()
                .type(type)
                .host((String) properties.get("storage.host"))
                .username((String) properties.get("storage.username"))
                .password((String) properties.get("storage.password"))
                .database((String) properties.get("storage.database"))
                .collection((String) properties.get("storage.collection"))
                .table((String) properties.get("storage.table"))
                .uri((String) properties.get("storage.uri"))
                .port(Integer.parseInt((String) properties.get("storage.port")))
                .build();


        switch (type) {
            case MONGODB -> storage = new DirectMongoFStorage<>(UUID.class, TestObject.class, credentials, TestObject::new);
            case SQLITE -> throw new UnsupportedOperationException("SQLite is not supported yet!");
            case MYSQL -> throw new UnsupportedOperationException("MySQL is not supported yet!");
            case MARIADB -> storage = new DirectMariaDBFStorage<>(UUID.class, TestObject.class, credentials, TestObject::new);
            case JSON -> throw new UnsupportedOperationException("JSON is not supported yet!");
            default -> throw new IllegalStateException("Unexpected value: " + type);
        }

        storage.deleteAll().join();
        storage.saveAll(initialData).join();
        storage.write().join();
    }

    private static Credentials credentials;
    private static FieldStorage<UUID, TestObject> storage;

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
    public void testTotalData2() {
        Collection<TestObject> join = storage.get().join();
        assertEquals(initialData.size(), join.size());
    }

    @Test
    public void testStartsWith() {
        Query query = Query.of().condition(Condition.of("data.address", "1", ConditionType.STARTS_WITH));
        Collection<TestObject> street = storage.get(query).join();
        assertEquals(4, street.size());
    }

    @Test
    public void testEndsWith() {
        Query query = Query.of().condition(Condition.of("name", "a", ConditionType.ENDS_WITH));
        Collection<TestObject> street = storage.get(query).join();
        assertEquals(5, street.size());

        Query query1 = Query.of().condition(Condition.of("data.phone", "0", ConditionType.ENDS_WITH));
        Collection<TestObject> phone = storage.get(query1).join();
        assertEquals(4, phone.size());
    }

    @Test
    public void testGreaterThan() {
        Query query = Query.of().condition(Condition.of("age", 20, ConditionType.GREATER_THAN));
        Collection<TestObject> street = storage.get(query).join();
        assertEquals(13, street.size());
    }

    @Test
    public void testLessThan() {
        Query query = Query.of().condition(Condition.of("age", 20, ConditionType.LESS_THAN));
        Collection<TestObject> street = storage.get(query).join();
        assertEquals(2, street.size());
    }

    @Test
    public void testContains() {
        Query query1 = Query.of()
                .condition(Condition.of("data.address", "Street", ConditionType.CONTAINS))
                .condition(Condition.of("age", 18, ConditionType.EQUALS));
        Collection<TestObject> street = storage.get(query1).join();
        assertEquals(1, street.size());

        Query query2 = Query.of()
                .condition(Condition.of("data.address", "Street", ConditionType.CONTAINS));
        Collection<TestObject> street1 = storage.get(query2).join();
        assertEquals(8, street1.size());

        Query query3 = Query.of()
                .condition(Condition.of("data.address", "Street", ConditionType.CONTAINS))
                .condition(Condition.or("data.address", "Avenue", ConditionType.CONTAINS))
                .condition(Condition.or("data.address", "Random Garbage", ConditionType.CONTAINS))
                .offset(0)
                .limit(20)
                .distinct(false);

        Collection<TestObject> allStreets = storage.get(query3).join();
        assertEquals(16, allStreets.size());
    }

    @Test
    public void testEquals() {
        Query query = Query.of()
                .condition(Condition.of("data.balance.currency", "USD", ConditionType.EQUALS));
        Collection<TestObject> usd = storage.get(query).join();
        assertEquals(15, usd.size());
    }

    @Test
    public void testNotEquals() {
        Query query = Query.of()
                .condition(Condition.of("data.balance.currency", "USD", ConditionType.NOT_EQUALS));
        Collection<TestObject> usd = storage.get(query).join();
        assertEquals(1, usd.size());
    }

    @Test
    public void testNotContains() {
        Query query = Query.of()
                .condition(Condition.of("data.address", "Street", ConditionType.NOT_CONTAINS));
        Collection<TestObject> street = storage.get(query).join();
        assertEquals(8, street.size());
    }

    @Test
    public void testNotStartsWith() {
        Query query = Query.of()
                .condition(Condition.of("data.address", "1", ConditionType.NOT_STARTS_WITH));
        Collection<TestObject> street = storage.get(query).join();
        assertEquals(12, street.size());
    }

    @Test
    public void testNotEndsWith() {
        Query query = Query.of()
                .condition(Condition.of("name", "a", ConditionType.NOT_ENDS_WITH));
        Collection<TestObject> street = storage.get(query).join();
        assertEquals(11, street.size());
    }

    @Test
    public void testLessThanOrEqualTo() {
        Query query = Query.of()
                .condition(Condition.of("age", 20, ConditionType.LESS_THAN_OR_EQUAL_TO));
        Collection<TestObject> street = storage.get(query).join();
        assertEquals(3, street.size());
    }

    @Test
    public void testGreaterThanOrEqualTo() {
        Query query = Query.of()
                .condition(Condition.of("age", 20, ConditionType.GREATER_THAN_OR_EQUAL_TO));
        Collection<TestObject> street = storage.get(query).join();
        assertEquals(14, street.size());
    }

    @Test
    public void testNotLessThan() {
        Query query = Query.of()
                .condition(Condition.of("age", 20, ConditionType.NOT_LESS_THAN));
        Collection<TestObject> street = storage.get(query).join();
        assertEquals(14, street.size());
    }

    @Test
    public void testNotGreaterThan() {
        Query query = Query.of()
                .condition(Condition.of("age", 20, ConditionType.NOT_GREATER_THAN));
        Collection<TestObject> street = storage.get(query).join();
        assertEquals(3, street.size());
    }

    @Test
    public void testNotLessThanOrEqualTo() {
        Query query = Query.of()
                .condition(Condition.of("age", 20, ConditionType.NOT_LESS_THAN_OR_EQUAL_TO));
        Collection<TestObject> street = storage.get(query).join();
        assertEquals(13, street.size());
    }

    @Test
    public void testNotGreaterThanOrEqualTo() {
        Query query = Query.of()
                .condition(Condition.of("age", 20, ConditionType.NOT_GREATER_THAN_OR_EQUAL_TO));
        Collection<TestObject> street = storage.get(query).join();
        assertEquals(2, street.size());
    }

    @Test
    public void testAnd() {
        Query query = Query.of()
                .condition(Condition.of("age", 20, ConditionType.GREATER_THAN))
                .condition(Condition.of("data.address", "Street", ConditionType.CONTAINS));
        Collection<TestObject> street = storage.get(query).join();
        assertEquals(6, street.size());
    }

    @Test
    public void testLimit() {
        Query query = Query.of()
                .limit(10)
                .condition(Condition.of("age", 20, ConditionType.GREATER_THAN_OR_EQUAL_TO));
        Collection<TestObject> street = storage.get(query).join();
        assertEquals(10, street.size());
    }

}
