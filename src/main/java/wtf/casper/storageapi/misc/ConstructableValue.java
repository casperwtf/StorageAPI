package wtf.casper.storageapi.misc;

public interface ConstructableValue<K, V> {

    default V constructValue(final K key) {
        throw new RuntimeException("ConstructableValue#constructValue(K) is not implemented! Please implement this method or use ConstructableValue#constructValue() instead.");
    }

    default V constructValue() {
        throw new RuntimeException("ConstructableValue#constructValue() is not implemented! Please implement this method or use ConstructableValue#constructValue() instead.");
    }

}
