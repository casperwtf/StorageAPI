package wtf.casper.storageapi.impl.direct.fstorage;

import wtf.casper.storageapi.Credentials;
import wtf.casper.storageapi.impl.fstorage.MongoFStorage;
import wtf.casper.storageapi.misc.ConstructableValue;
import wtf.casper.storageapi.misc.KeyValue;

import java.util.function.Function;
import java.util.function.Supplier;

public class DirectMongoFStorage<K, V> extends MongoFStorage<K, V> implements ConstructableValue<K, V>, KeyValue<K, V> {

    private final Function<K, V> function;
    private final Supplier<V> supplier;
    private final Class<K> keyClass;


    public DirectMongoFStorage(Class<K> keyClass, Class<V> valueClass, Credentials credentials, Function<K, V> function, Supplier<V> supplier) {
        super(valueClass, credentials);
        this.function = function;
        this.supplier = supplier;
        this.keyClass = keyClass;
    }


    @Override
    public V constructValue(K key) {
        return function.apply(key);
    }

    @Override
    public V constructValue() {
        return supplier.get();
    }

    @Override
    public Class<K> key() {
        return keyClass;
    }

    @Override
    public Class<V> value() {
        return valueClass;
    }
}
