package wtf.casper.storageapi.impl.direct.kvstorage;

import wtf.casper.storageapi.Credentials;
import wtf.casper.storageapi.impl.kvstorage.SQLKVStorage;
import wtf.casper.storageapi.misc.ConstructableValue;

import java.util.function.Function;

public class DirectSQLKVStorage<K, V> extends SQLKVStorage<K, V> implements ConstructableValue<K, V> {

    private final Function<K, V> function;

    public DirectSQLKVStorage(Class<K> keyClass, Class<V> valueClass, Credentials credentials, Function<K, V> function) {
        super(keyClass, valueClass, credentials.getTable(), credentials);
        this.function = function;
    }


    @Override
    public V constructValue(K key) {
        return function.apply(key);
    }
}
