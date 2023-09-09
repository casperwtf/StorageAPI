package wtf.casper.storageapi.impl.direct.kvstorage;

import wtf.casper.storageapi.Credentials;
import wtf.casper.storageapi.impl.kvstorage.MariaDBKVStorage;
import wtf.casper.storageapi.misc.ConstructableValue;

import java.util.function.Function;

public class DirectMariaDBKVStorage<K, V> extends MariaDBKVStorage<K, V> implements ConstructableValue<K, V> {

    private final Function<K, V> function;

    public DirectMariaDBKVStorage(Class<K> keyClass, Class<V> valueClass, Credentials credentials, Function<K, V> function) {
        super(keyClass, valueClass, credentials);
        this.function = function;
    }


    @Override
    public V constructValue(K key) {
        return function.apply(key);
    }
}
