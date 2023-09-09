package wtf.casper.storageapi.impl.direct.kvstorage;

import wtf.casper.storageapi.impl.fstorage.JsonFStorage;
import wtf.casper.storageapi.impl.kvstorage.JsonKVStorage;
import wtf.casper.storageapi.misc.ConstructableValue;

import java.io.File;
import java.util.function.Function;

public class DirectJsonKVStorage<K, V> extends JsonKVStorage<K, V> implements ConstructableValue<K, V> {

    private final Function<K, V> function;

    public DirectJsonKVStorage(Class<K> keyClass, Class<V> valueClass, File file, Function<K, V> function) {
        super(file, keyClass, valueClass);
        this.function = function;
    }

    @Override
    public V constructValue(K key) {
        return function.apply(key);
    }
}
