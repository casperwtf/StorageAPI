package wtf.casper.storageapi.misc;

import org.objenesis.Objenesis;
import wtf.casper.storageapi.utils.Constants;

public interface ConstructableValue<K, V> extends KeyValue<K, V> {

    default V constructValue(final K key) {
        throw new RuntimeException("ConstructableValue#constructValue(K) is not implemented! Please implement this method or use ConstructableValue#constructValue() instead.");
    }

    default V constructValue() {
        return Constants.OBJENESIS_STD.newInstance(value());
    }
}
