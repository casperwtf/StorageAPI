# Description
StorageAPI is an easy way to query and store data.

# Useful Stuff We Use

YourKit supports open source projects with innovative and intelligent tools
for monitoring and profiling Java and .NET applications.
YourKit is the creator of (YourKit Java Profiler)[https://www.yourkit.com/java/profiler/],
(YourKit .NET Profiler)[https://www.yourkit.com/dotnet-profiler/],
and (YourKit YouMonitor)[https://www.yourkit.com/youmonitor/].

StorageAPI is setup to be ran asynchronously, so you don't have to worry about blocking the main thread. This is done through the java completable future API.
# Create credentials object
```java
//Spigot/YAML Configuration method to make configuration
StorageType type = StorageType.valueOf(plugin.getConfig().getString("storage.type"));
credentials = Credentials.of(
        type,
        plugin.getConfig().getString("storage.host"),
        plugin.getConfig().getString("storage.username"),
        plugin.getConfig().getString("storage.password"),
        plugin.getConfig().getString("storage.database"),
        plugin.getConfig().getString("storage.collection"),
        plugin.getConfig().getString("storage.table"),
        plugin.getConfig().getString("storage.uri"),
        plugin.getConfig().getInt("storage.port")
);
// You can also supply the potential configuration options somewhere else such as environment variables or a file.
```

# Usage of KVStorage
KVStorage is a key value storage system. It is a simple way to store data.
This supports all storage types (Json, MongoDB, SQL, MariaDB, SQLite)
```java
//Create a new KVStorage instance
KVStorage<K, V> storage;
switch (type) {
    case MONGODB -> storage = new DirectMongoKVStorage<>(KeyClass.class, ValueClass.class, credentials);
    case JSON -> storage = new DirectJsonKVStorage<>(KeyClass.class, ValueClass.class, new File(plugin.getDataFolder()+File.separator+"data.json"));
    default -> throw new IllegalStateException("Unexpected value: " + type);
}
```

```java
// Querying the storage
// All this is based off the test code in the test package
storage.get(some uuid or string).whenComplete((value, throwable) -> {
    // value is the object or null
});

storage.getOrDefault(some uuid or string).whenComplete((value, throwable) -> {
    // value is the object or the default
});

storage.allValues().whenComplete((value, throwable) -> {
    // value is a list of objects
});

//Save an object
storage.save(...); // returns a completable future of void so you can do a when complete when its done
storage.saveAll(...);
        
//Delete an object
storage.remove(/* the object itself */); // returns a completable future of void so you can do a when complete when its done

//Migrate an storage type to another storage type
storage.migrate(oldStorageObj);

storage.contains(some uuid or string).whenComplete((value, throwable) -> {
    // value is a boolean
});
```

# Usage of FieldStorage
FieldStorage is a more advanced way to store data. It allows you to query the data based on fields in the object.
SQL is not supported for FieldStorage *yet*.
```java
// Create a new FieldStorage instance
FieldStorage<K, V> storage;
switch (type) {
    case MONGODB -> storage = new DirectMongoFStorage<>(KeyClass.class, ValueClass.class, credentials, TestObject::new); // Last constructor arg is a function to create a new instance of the value class
    case JSON -> storage = new DirectJsonFStorage<>(KeyClass.class, ValueClass.class, new File(plugin.getDataFolder()+File.separator+"data.json"), TestObject::new); // The JSON equivalent
    default -> throw new IllegalStateException("Unexpected value: " + type);
}
```

```java
// Querying the storage
// All this is based off the test code in the test package

// Get all objects with the name "Jane"
storage.get("name", "Jane").whenComplete((value, throwable) -> {
    // value is a list of objects or null
});

// Get all objects with the name "Jane" and age 20
storage.get(
        Filter.of("name", "Jane", FilterType.EQUALS),
        Filter.of("age", 20, FilterType.EQUALS)
).whenComplete((value, throwable) -> {
    // value is a list of objects
});

// Get the first 10 objects with the name "Jane" and age 20
storage.get(10,
        Filter.of("name", "Jane", FilterType.EQUALS),
        Filter.of("age", 20, FilterType.EQUALS)
).whenComplete((value, throwable) -> {
        // value is a list of objects
});

// Filter objects from subobject values and sort the output and get the first 3
storage.get(3,
        Filter.of("name", "Jane", FilterType.EQUALS, SortType.ASCENDING, Filter.Type.AND),
        Filter.of("age", 20, FilterType.EQUALS, SortType.NONE, Filter.Type.AND),
        Filter.of("data.age", 20, FilterType.EQUALS, SortType.NONE, Filter.Type.AND)
).whenComplete((value, throwable) -> {
    // value is a list of objects
});

// Get an object by the key
storage.get(some uuid or string).whenComplete((value, throwable) -> {
    // value is the object or null
});

// Get an object by key or default
storage.getOrDefault(some uuid or string).whenComplete((value, throwable) -> {
    // value is the object or the default
});

// Get all objects
storage.allValues().whenComplete((value, throwable) -> {
    // value is a list of objects
});

//Save an object
storage.save(...); // returns a completable future of void so you can do a when complete when its done
storage.saveAll(...);

//Migrate an storage type to another storage type
storage.migrate(oldStorageObj);

//Delete an object
storage.remove(/* the object itself */); // returns a completable future of void so you can do a when complete when its done

```