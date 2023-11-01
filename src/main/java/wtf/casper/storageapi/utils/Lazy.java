package wtf.casper.storageapi.utils;

public class Lazy<T> {
    private final Supplier<T> supplier;
    private T value;

    public Lazy(final Supplier<T> supplier) {
        this.supplier = supplier;
    }

    public T get() {
        if (value == null) {
            value = supplier.get();
        }

        return value;
    }

    public interface Supplier<T> {
        T get();
    }
}
