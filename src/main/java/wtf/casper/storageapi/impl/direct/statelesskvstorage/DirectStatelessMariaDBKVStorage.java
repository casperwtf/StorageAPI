package wtf.casper.storageapi.impl.direct.statelesskvstorage;

import wtf.casper.storageapi.Credentials;
import wtf.casper.storageapi.impl.kvstorage.SqlKVStorage;
import wtf.casper.storageapi.misc.ConstructableValue;

import java.util.function.Function;

public class DirectStatelessMariaDBKVStorage<K, V> extends SqlKVStorage<K, V> implements ConstructableValue<K, V> {

    private final Function<K, V> function;

    public DirectStatelessMariaDBKVStorage(Class<K> keyClass, Class<V> valueClass, Credentials credentials, Function<K, V> function) {
        super(keyClass, valueClass, credentials);
        this.function = function;
    }


    @Override
    public V constructValue(K key) {
        return function.apply(key);
    }
}
