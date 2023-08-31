package wtf.casper.storageapi.impl.direct.statelessfstorage;

import wtf.casper.storageapi.Credentials;
import wtf.casper.storageapi.impl.statelessfstorage.StatelessMongoFStorage;
import wtf.casper.storageapi.misc.ConstructableValue;
import wtf.casper.storageapi.misc.KeyValue;

import java.util.function.Function;
import java.util.function.Supplier;

public class DirectStatelessMongoFStorage<K, V> extends StatelessMongoFStorage<K, V> implements ConstructableValue<K, V> {

    private final Function<K, V> function;

    public DirectStatelessMongoFStorage(Class<K> keyClass, Class<V> valueClass, Credentials credentials, Function<K, V> function) {
        super(keyClass, valueClass, credentials);
        this.function = function;
    }

    @Override
    public V constructValue(K key) {
        return function.apply(key);
    }
}
