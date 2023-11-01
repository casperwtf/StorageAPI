package wtf.casper.storageapi;

import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import wtf.casper.storageapi.utils.ReflectionUtil;

import java.util.*;

@Getter
public enum FilterType {
    EQUALS(Object.class),
    CONTAINS(String.class),
    STARTS_WITH(String.class),
    ENDS_WITH(String.class),
    GREATER_THAN(Number.class),
    LESS_THAN(Number.class),
    GREATER_THAN_OR_EQUAL_TO(Number.class),
    LESS_THAN_OR_EQUAL_TO(Number.class),
    NOT_EQUALS(Object.class),
    NOT_CONTAINS(String.class),
    NOT_STARTS_WITH(String.class),
    NOT_ENDS_WITH(String.class),
    NOT_GREATER_THAN(Number.class),
    NOT_LESS_THAN(Number.class),
    NOT_GREATER_THAN_OR_EQUAL_TO(Number.class),
    NOT_LESS_THAN_OR_EQUAL_TO(Number.class);

    private final Class<?>[] types;

    FilterType(Class<?>... types) {
        this.types = types;
    }

    public boolean isApplicable(@NotNull final Class<?> type) {
        for (final Class<?> clazz : getTypes()) {
            if (type.isAssignableFrom(clazz) || clazz.isAssignableFrom(type)) {
                return true;
            }
        }
        return false;
    }

    /**
     * @param object    the object we are checking.
     * @param fieldName the name of the field we are checking.
     * @param value     the value we are checking for.
     * @return true if the object passes the filter, false otherwise.
     */
    public boolean passes(Object object, String fieldName, Object value) {
        Object field = object;
        if (fieldName.contains(".")) {
            String[] split = fieldName.split("\\.");
            for (int i = 0; i < split.length; i++) {

                Optional<Object> o = ReflectionUtil.getFieldValue(field, split[i]);
                if (o.isEmpty()) {
                    return false;
                }
                field = o.get();
                if (field instanceof Collection<?> collection) {
                    String remainingSearch = String.join(".", Arrays.copyOfRange(split, i + 1, split.length));
                    for (Object o1 : collection) {
                        if (passes(o1, remainingSearch, value)) {
                            return true;
                        }
                    }
                    return false;
                }
                if (field.getClass().isArray()) {
                    String remainingSearch = String.join(".", Arrays.copyOfRange(split, i + 1, split.length));
                    for (Object o1 : (Object[]) field) {
                        if (passes(o1, remainingSearch, value)) {
                            return true;
                        }
                    }
                    return false;
                }
                if (field instanceof Map<?, ?> map) {
                    String remainingSearch = String.join(".", Arrays.copyOfRange(split, i + 1, split.length));
                    for (Map.Entry<?, ?> entry : map.entrySet()) {
                        if (passes(entry.getValue(), remainingSearch, value)) {
                            return true;
                        }
                        if (passes(entry.getKey(), remainingSearch, value)) {
                            return true;
                        }
                    }
                    return false;
                }
            }
        } else {
            Optional<Object> o = ReflectionUtil.getFieldValue(object, fieldName);
            if (o.isEmpty()) {
                return false;
            }
            field = o.get();
        }


        if (!isApplicable(field.getClass()) && !(field instanceof Collection) && !(field instanceof Map)) {
            return false;
        }

        if (field instanceof Collection<?> collection) {
            for (Object o : collection) {
                if (check(o, value)) {
                    return true;
                }
            }
        }

        if (field instanceof Map<?, ?> map) {
            for (Object o : map.values()) {
                if (check(o, value)) {
                    return true;
                }
            }

            for (Object o : map.keySet()) {
                if (check(o, value)) {
                    return true;
                }
            }
        }

        if (fieldName.getClass().isArray()) {
            assert field instanceof Object[];
            for (Object o : (Object[]) field) {
                if (check(o, value)) {
                    return true;
                }
            }
        }

        return check(field, value);
    }

    private boolean check(Object fieldValue, Object value) {
        switch (this) {
            case EQUALS -> {
                return Objects.equals(fieldValue, value);
            }
            case CONTAINS -> {
                return fieldValue.toString().contains(value.toString());
            }
            case STARTS_WITH -> {
                return fieldValue.toString().startsWith(value.toString());
            }
            case ENDS_WITH -> {
                return fieldValue.toString().endsWith(value.toString());
            }
            case GREATER_THAN -> {
                return ((Number) fieldValue).doubleValue() > ((Number) value).doubleValue();
            }
            case LESS_THAN -> {
                return ((Number) fieldValue).doubleValue() < ((Number) value).doubleValue();
            }
            case GREATER_THAN_OR_EQUAL_TO -> {
                return ((Number) fieldValue).doubleValue() >= ((Number) value).doubleValue();
            }
            case LESS_THAN_OR_EQUAL_TO -> {
                return ((Number) fieldValue).doubleValue() <= ((Number) value).doubleValue();
            }
            case NOT_EQUALS -> {
                return !fieldValue.equals(value);
            }
            case NOT_CONTAINS -> {
                return !fieldValue.toString().contains(value.toString());
            }
            case NOT_STARTS_WITH -> {
                return !fieldValue.toString().startsWith(value.toString());
            }
            case NOT_ENDS_WITH -> {
                return !fieldValue.toString().endsWith(value.toString());
            }
            case NOT_GREATER_THAN -> {
                return !(((Number) fieldValue).doubleValue() > ((Number) value).doubleValue());
            }
            case NOT_LESS_THAN -> {
                return !(((Number) fieldValue).doubleValue() < ((Number) value).doubleValue());
            }
            case NOT_GREATER_THAN_OR_EQUAL_TO -> {
                return !(((Number) fieldValue).doubleValue() >= ((Number) value).doubleValue());
            }
            case NOT_LESS_THAN_OR_EQUAL_TO -> {
                return !(((Number) fieldValue).doubleValue() <= ((Number) value).doubleValue());
            }
            default -> {
                return false;
            }
        }
    }

}
