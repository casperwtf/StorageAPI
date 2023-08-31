package wtf.casper.storageapi.impl.direct.fstorage;

import wtf.casper.storageapi.impl.fstorage.JsonFStorage;
import wtf.casper.storageapi.misc.ConstructableValue;
import wtf.casper.storageapi.misc.KeyValue;

import java.io.File;
import java.util.function.Function;
import java.util.function.Supplier;

public class DirectJsonFStorage<K, V> extends JsonFStorage<K, V> implements ConstructableValue<K, V> {

    private final Function<K, V> function;

    public DirectJsonFStorage(Class<K> keyClass, Class<V> valueClass, File file, Function<K, V> function) {
        super(file, keyClass, valueClass);
        this.function = function;
    }

    @Override
    public V constructValue(K key) {
        return function.apply(key);
    }
}
