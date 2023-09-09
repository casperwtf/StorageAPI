package wtf.casper.storageapi.impl.direct.statelesskvstorage;

import wtf.casper.storageapi.Credentials;
import wtf.casper.storageapi.impl.kvstorage.MongoKVStorage;
import wtf.casper.storageapi.impl.statelesskvstorage.StatelessMongoKVStorage;
import wtf.casper.storageapi.misc.ConstructableValue;

import java.util.function.Function;

public class DirectStatelessMongoKVStorage<K, V> extends StatelessMongoKVStorage<K, V> implements ConstructableValue<K, V> {

    private final Function<K, V> function;


    public DirectStatelessMongoKVStorage(Class<K> keyClass, Class<V> valueClass, Credentials credentials, Function<K, V> function) {
        super(keyClass, valueClass, credentials);
        this.function = function;
    }

    @Override
    public V constructValue(K key) {
        return function.apply(key);
    }
}
