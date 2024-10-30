package wtf.casper.storageapi;

import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import wtf.casper.storageapi.utils.ReflectionUtil;

import java.util.*;
import java.util.stream.Collectors;

@Getter
public enum SortingType {
    NONE(Object.class),
    ASCENDING(String.class, Number.class, Boolean.class),
    DESCENDING(String.class, Number.class, Boolean.class);

    private final Class<?>[] types;

    SortingType(Class<?>... types) {
        this.types = types;
    }

    public boolean isApplicable(@NotNull final Class<?> type) {
        for (final Class<?> clazz : types) {
            if (clazz.isAssignableFrom(type)) {
                return true;
            }
        }
        return false;
    }
}
