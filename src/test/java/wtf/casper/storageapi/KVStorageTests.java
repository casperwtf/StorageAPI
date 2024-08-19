package wtf.casper.storageapi;

import lombok.extern.java.Log;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import wtf.casper.storageapi.impl.direct.kvstorage.*;

import java.io.File;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

@Log
public class KVStorageTests {

    @BeforeAll
    public static void setup() {
        InputStream stream = KVStorageTests.class.getClassLoader().getResourceAsStream("storage.properties");
        File file = new File("."+File.separator+"storage.properties");
        if (file.exists()) {
            stream = file.toURI().toASCIIString().contains("jar") ? KVStorageTests.class.getClassLoader().getResourceAsStream("storage.properties") : null;
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
            case MONGODB -> storage = new DirectMongoKVStorage<>(UUID.class, TestObject.class, credentials, TestObject::new);
            case SQLITE -> storage = new DirectSQLiteKVStorage<>(UUID.class, TestObject.class, new File("src/test/resources/data.db"), "data", TestObject::new);
            case SQL -> storage = new DirectSQLKVStorage<>(UUID.class, TestObject.class, credentials, TestObject::new);
            case MARIADB -> storage = new DirectMariaDBKVStorage<>(UUID.class, TestObject.class, credentials, TestObject::new);
            case JSON -> storage = new DirectJsonKVStorage<>(UUID.class, TestObject.class, new File("./src/test/resources/data"), TestObject::new);
            default -> throw new IllegalStateException("Unexpected value: " + type);
        }

        switch (type) {
            case MONGODB -> storageOther = new DirectMongoKVStorage<>(UUID.class, TestObjectOther.class, credentials, TestObjectOther::new);
            case SQLITE -> storageOther = new DirectSQLiteKVStorage<>(UUID.class, TestObjectOther.class, new File("src/test/resources/data.db"), "data", TestObjectOther::new);
            case SQL -> storageOther = new DirectSQLKVStorage<>(UUID.class, TestObjectOther.class, credentials, TestObjectOther::new);
            case MARIADB -> storageOther = new DirectMariaDBKVStorage<>(UUID.class, TestObjectOther.class, credentials, TestObjectOther::new);
            case JSON -> storageOther = new DirectJsonKVStorage<>(UUID.class, TestObjectOther.class, new File("./src/test/resources/data"), TestObjectOther::new);
            default -> throw new IllegalStateException("Unexpected value: " + type);
        }

        storage.deleteAll().join();
        storage.saveAll(initialData).join();
        storage.write().join();
    }

    private static Credentials credentials;
    private static KVStorage<UUID, TestObject> storage;
    private static KVStorage<UUID, TestObjectOther> storageOther;

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
                            19, new TestObjectBalance(200, "USD")
                    )
            )
    );

    
    @Test
    public void testTotalData() {
        assertEquals(initialData.size(), storage.allValues().join().size());
        log.fine(" --- Total data test passed!");
    }

    @Test
    public void testSave() {
        TestObject testObject = new TestObject(
                UUID.fromString("00000000-0000-0000-0000-000000000016"), "Test", 100,
                new TestObjectData("1234 Test Street", "Test Employer", "test@test", "123-456-7890",
                        100, new TestObjectBalance(100, "USD")
                )
        );

        storage.save(testObject).join();
        assertEquals(testObject, storage.get(testObject.getId()).join());

        storage.remove(testObject).join();
        assertEquals(null, storage.get(testObject.getId()).join());
    }

    @Test
    public void testRemove() {
        TestObject testObject = new TestObject(
                UUID.fromString("00000000-0000-0000-0000-000000000017"), "Test", 100,
                new TestObjectData("1234 Test Street", "Test Employer", "test@test", "123-456-7890",
                        100, new TestObjectBalance(100, "USD")
                )
        );

        storage.save(testObject).join();
        assertEquals(testObject, storage.get(testObject.getId()).join());
        storage.remove(testObject).join();
        assertEquals(null, storage.get(testObject.getId()).join());
    }

    @Test
    public void testWrite() {
        TestObject testObject = new TestObject(
                UUID.fromString("00000000-0000-0000-0000-000000000018"), "Test", 100,
                new TestObjectData("1234 Test Street", "Test Employer", "test@test", "123-456-7890",
                        100, new TestObjectBalance(100, "USD")
                )
        );

        storage.save(testObject).join();
        assertEquals(testObject, storage.get(testObject.getId()).join());
        storage.write().join();
        assertEquals(testObject, storage.get(testObject.getId()).join());
        storage.remove(testObject).join();
        assertEquals(null, storage.get(testObject.getId()).join());
    }

    @Test
    public void testDeleteAll() {
        storage.deleteAll().join();
        assertEquals(0, storage.allValues().join().size());
    }

    @Test
    public void testChangeFieldName() {
        TestObject testObject = new TestObject(
                UUID.fromString("00000000-0000-0000-0000-000000000019"), "Test", 100,
                new TestObjectData("1234 Test Street", "Test Employer", "test@test", "123-456-7890",
                        100, new TestObjectBalance(100, "USD")
                )
        );

        storage.save(testObject).join();

        storage.renameField("id", "idOther").join();
        storage.renameField("name", "nameOther").join();
        storage.renameField("age", "ageOther").join();

        TestObjectOther join;
        try {
             join = storageOther.get(UUID.fromString("00000000-0000-0000-0000-000000000019")).join();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (join == null) {
            throw new RuntimeException("Join is null");
        }

        assertEquals(testObject.getId(), join.getIdOther());
        assertEquals(testObject.getName(), join.getNameOther());
        assertEquals(testObject.getAge(), join.getAgeOther());
        assertEquals(testObject.getData().getAge(), join.getData().getAge());

        storage.renameField("idOther", "id").join();
        storage.renameField("nameOther", "name").join();
        storage.renameField("ageOther", "age").join();

        TestObject object = storage.get(testObject.getId()).join();
        assertEquals(testObject, object);

        storageOther.remove(join).join();
    }

    @Test
    public void testChangeFieldNames() {
        TestObject testObject = new TestObject(
                UUID.fromString("00000000-0000-0000-0000-000000000019"), "Test", 100,
                new TestObjectData("1234 Test Street", "Test Employer", "test@test", "123-456-7890",
                        100, new TestObjectBalance(100, "USD")
                )
        );

        storage.save(testObject).join();

        storage.renameFields(Map.of(
                "id", "idOther",
                "name", "nameOther",
                "age", "ageOther"
        )).join();

        TestObjectOther join;
        try {
            join = storageOther.get(UUID.fromString("00000000-0000-0000-0000-000000000019")).join();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (join == null) {
            throw new RuntimeException("Join is null");
        }

        assertEquals(testObject.getId(), join.getIdOther());
        assertEquals(testObject.getName(), join.getNameOther());
        assertEquals(testObject.getAge(), join.getAgeOther());
        assertEquals(testObject.getData().getAge(), join.getData().getAge());

        storage.renameFields(Map.of(
                "idOther", "id",
                "nameOther", "name",
                "ageOther", "age"
        )).join();

        TestObject object = storage.get(testObject.getId()).join();
        assertEquals(testObject, object);

        storage.remove(testObject).join();
    }

}
