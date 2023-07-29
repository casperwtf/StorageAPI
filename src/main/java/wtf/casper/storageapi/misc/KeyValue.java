package wtf.casper.storageapi.misc;

public interface KeyValue<K, V> {
    Class<K> key();

    Class<V> value();
}
