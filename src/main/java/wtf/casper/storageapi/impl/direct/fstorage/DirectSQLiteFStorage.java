package wtf.casper.storageapi.impl.direct.fstorage;

import wtf.casper.storageapi.impl.fstorage.SQLiteFStorage;
import wtf.casper.storageapi.misc.ConstructableValue;

import java.io.File;
import java.util.function.Function;

public class DirectSQLiteFStorage<K, V> extends SQLiteFStorage<K, V> implements ConstructableValue<K, V> {

    private final Function<K, V> function;

    public DirectSQLiteFStorage(Class<K> keyClass, Class<V> valueClass, File file, String table, Function<K, V> function) {
        super(keyClass, valueClass, file, table);
        this.function = function;
    }


    @Override
    public V constructValue(K key) {
        return function.apply(key);
    }
}
