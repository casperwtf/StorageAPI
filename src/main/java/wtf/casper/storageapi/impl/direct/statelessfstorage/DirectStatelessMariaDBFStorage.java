package wtf.casper.storageapi.impl.direct.statelessfstorage;

import wtf.casper.storageapi.Credentials;
import wtf.casper.storageapi.impl.statelessfstorage.StatelessMariaDBFStorage;
import wtf.casper.storageapi.misc.ConstructableValue;

import java.util.function.Function;

public class DirectStatelessMariaDBFStorage<K, V> extends StatelessMariaDBFStorage<K, V> implements ConstructableValue<K, V> {

    private final Function<K, V> function;

    public DirectStatelessMariaDBFStorage(Class<K> keyClass, Class<V> valueClass, Credentials credentials, Function<K, V> function) {
        super(keyClass, valueClass, credentials);
        this.function = function;
    }

    @Override
    public V constructValue(K key) {
        return function.apply(key);
    }
}
