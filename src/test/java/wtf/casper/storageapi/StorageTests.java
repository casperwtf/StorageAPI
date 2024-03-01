package wtf.casper.storageapi;

import lombok.extern.java.Log;
import wtf.casper.storageapi.impl.direct.fstorage.*;

import java.io.File;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.CompletableFuture;

@Log
public class StorageTests {

    public static void main(String[] args) {
        InputStream stream = StorageTests.class.getClassLoader().getResourceAsStream("storage.properties");
        File file = new File("."+File.separator+"storage.properties");
        if (file.exists()) {
            stream = file.toURI().toASCIIString().contains("jar") ? StorageTests.class.getClassLoader().getResourceAsStream("storage.properties") : null;
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

        new StorageTests(properties);
    }

    public StorageTests(Properties properties) {
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
                (Integer) properties.get("storage.port")
        );


        switch (type) {
            case MONGODB -> storage = new DirectMongoFStorage<>(UUID.class, TestObject.class, credentials, TestObject::new);
            case JSON -> storage = new DirectJsonFStorage<>(UUID.class, TestObject.class, new File("."+File.separator+"data.json"), TestObject::new);
            default -> throw new IllegalStateException("Unexpected value: " + type);
        }

        storage.deleteAll().join();
        loadData(storage);
        storage.write().join();

        testData(storage, initialData);
        storage.close();
    }

    private Credentials credentials;
    private FieldStorage<UUID, TestObject> storage;

    private final List<TestObject> initialData = List.of(
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
                            19, new TestObjectBalance(200, "USD")
                    )
            )
    );


    private void testData(StatelessFieldStorage<UUID, TestObject> storage, List<TestObject> originalData) {
        log.fine(" --- Testing data...");
        for (Method declaredMethod : getClass().getDeclaredMethods()) {
            if (declaredMethod.isAnnotationPresent(Test.class)) {
                try {
                    declaredMethod.invoke(this, storage, originalData);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    private void testTotalData(StatelessFieldStorage<UUID, TestObject> storage, List<TestObject> originalData) {
        log.fine(" --- Testing total data...");
        assertEquals(storage.allValues().join().size(), originalData.size());
        log.fine(" --- Total data test passed!");
    }

    @Test
    public void testStartsWith(StatelessFieldStorage<UUID, TestObject> storage, List<TestObject> originalData) {
        log.fine(" --- Testing filter starts with...");

        Collection<TestObject> street = storage.get(
                Filter.of("data.address", "1", FilterType.STARTS_WITH)
        ).join();
        assertEquals(street.size(), 4);

        log.fine(" --- Filter starts with test passed!");
    }

    @Test
    public void testEndsWith(StatelessFieldStorage<UUID, TestObject> storage, List<TestObject> originalData) {
        log.fine(" --- Testing filter ends with...");

        Collection<TestObject> street = storage.get(
                Filter.of("name", "a", FilterType.ENDS_WITH)
        ).join();
        assertEquals(street.size(), 5);

        Collection<TestObject> phone = storage.get(
                Filter.of("data.phone", "0", FilterType.ENDS_WITH)
        ).join();
        assertEquals(phone.size(), 4);

        log.fine(" --- Filter starts with test passed!");
    }

    @Test
    public void testGreaterThan(StatelessFieldStorage<UUID, TestObject> storage, List<TestObject> originalData) {
        log.fine(" --- Testing filter greater than...");

        Collection<TestObject> street = storage.get(
                Filter.of("age", 20, FilterType.GREATER_THAN)
        ).join();
        assertEquals(street.size(), 13);

        log.fine(" --- Filter greater than test passed!");
    }

    @Test
    public void testLessThan(StatelessFieldStorage<UUID, TestObject> storage, List<TestObject> originalData) {
        log.fine(" --- Testing filter less than...");

        Collection<TestObject> street = storage.get(
                Filter.of("age", 20, FilterType.LESS_THAN)
        ).join();
        assertEquals(street.size(), 2);

        log.fine(" --- Filter less than test passed!");
    }

    @Test
    private void testContains(StatelessFieldStorage<UUID, TestObject> storage, List<TestObject> originalData) {
        log.fine(" --- Testing filter contains...");

        Collection<TestObject> street = storage.get(
                Filter.of("data.address", "Street", FilterType.CONTAINS),
                Filter.of("age", 18, FilterType.EQUALS)
        ).join();
        assertEquals(street.size(), 1);

        Collection<TestObject> street1 = storage.get(
                Filter.of("data.address", "Street", FilterType.CONTAINS)
        ).join();
        assertEquals(street1.size(), 8);

        CompletableFuture<Collection<TestObject>> allStreets = storage.get(
                Filter.of("data.address", "Street", FilterType.CONTAINS),
                Filter.of("data.address", "Avenue", FilterType.CONTAINS, SortingType.NONE, Filter.Type.OR)
        );
        assertEquals(allStreets.join().size(), 16);
        log.fine(" --- Filter contains test passed!");
    }

    @Test
    private void testEquals(StatelessFieldStorage<UUID, TestObject> storage, List<TestObject> originalData) {
        log.fine(" --- Testing filter equals...");

        Collection<TestObject> usd = storage.get(
                Filter.of("data.balance.currency", "USD", FilterType.EQUALS)
        ).join();
        assertEquals(usd.size(), 16);
    }



    private void assertEquals(Object actual, Object expected) {
        if (!Objects.equals(expected, actual)) {
            throw new AssertionError("Expected: " + expected + ", but was: " + actual);
        }
    }

    private void loadData(StatelessFieldStorage storage) {
        storage.saveAll(initialData).join();
    }
}
