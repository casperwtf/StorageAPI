package wtf.casper.storageapi.utils;

import java.util.function.Consumer;

@FunctionalInterface
public interface UnsafeConsumer<T> extends Consumer<T> {

    @Override
    default void accept(final T t) {

        try {
            this.acceptThrow(t);
        } catch (final Exception exception) {
            exception.printStackTrace();
        }

    }

    void acceptThrow(final T t) throws Exception;

}
