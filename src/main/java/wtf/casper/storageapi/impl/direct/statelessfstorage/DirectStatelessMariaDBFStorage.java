package wtf.casper.storageapi.impl.direct.statelessfstorage;

import wtf.casper.storageapi.Credentials;
import wtf.casper.storageapi.impl.statelessfstorage.StatelessMariaDBStorage;
import wtf.casper.storageapi.misc.ConstructableValue;
import wtf.casper.storageapi.misc.KeyValue;

import java.util.function.Function;
import java.util.function.Supplier;

public class DirectStatelessMariaDBFStorage<K, V> extends StatelessMariaDBStorage<K, V> implements ConstructableValue<K, V>, KeyValue<K, V> {

    private final Function<K, V> function;
    private final Supplier<V> supplier;

    public DirectStatelessMariaDBFStorage(Class<K> keyClass, Class<V> valueClass, Credentials credentials, Function<K, V> function, Supplier<V> supplier) {
        super(keyClass, valueClass, credentials);
        this.function = function;
        this.supplier = supplier;
    }


    @Override
    public V constructValue(K key) {
        return function.apply(key);
    }

    @Override
    public V constructValue() {
        return supplier.get();
    }
}
