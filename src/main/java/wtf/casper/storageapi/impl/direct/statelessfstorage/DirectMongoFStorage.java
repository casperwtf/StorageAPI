package wtf.casper.storageapi.impl.direct.statelessfstorage;

import wtf.casper.storageapi.Credentials;
import wtf.casper.storageapi.impl.fstorage.MongoFStorage;
import wtf.casper.storageapi.misc.ConstructableValue;

import java.util.function.Function;

public class DirectMongoFStorage<K, V> extends MongoFStorage<K, V> implements ConstructableValue<K, V> {

    private final Function<K, V> function;

    public DirectMongoFStorage(Class<K> keyClass, Class<V> valueClass, Credentials credentials, Function<K, V> function) {
        super(keyClass, valueClass, credentials);
        this.function = function;
    }

    @Override
    public V constructValue(K key) {
        return function.apply(key);
    }
}
