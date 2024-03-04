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

    // this needs to be turned into in-query sorting instead of in-memory sorting
    public <V> Collection<V> sort(Collection<V> values, String field) {
        if (values.isEmpty() || this == NONE) {
            return values;
        }

        values = new ArrayList<>(values);

        // Get the first value and check if the field exists.
        V next = values.iterator().next();
        Optional<Object> fieldValue = ReflectionUtil.getFieldValue(next, field);
        if (fieldValue.isEmpty()) {
            throw new IllegalArgumentException("Field " + field + " does not exist in " + next.getClass().getSimpleName());
        }

        // Check if the field is of a valid type for sorting.
        final Object o = fieldValue.get();
        if (!isApplicable(o.getClass()) && !(o instanceof Map<?, ?>)) {
            throw new IllegalArgumentException("Field " + field + " is not of a valid type for sorting.");
        }

        // Sort the values if map
        if (o instanceof Map<?, ?>) {
            // check if map key and value are sortable
            Iterator<? extends Map.Entry<?, ?>> entryIterator = ((Map<?, ?>) o).entrySet().iterator();
            Map.Entry<?, ?> entry = entryIterator.next();
            if (!isApplicable(entry.getKey().getClass()) || !isApplicable(entry.getValue().getClass())) {
                throw new IllegalArgumentException("Field " + field + " is not of a valid type for sorting.");
            }

            // we prefer to sort by value because the key is usually the id
            boolean useValue = isApplicable(entry.getValue().getClass());

            if (useValue) {
                values = values.stream().sorted((o1, o2) -> {
                    Optional<Object> fieldValue1 = ReflectionUtil.getFieldValue(o1, field);
                    if (fieldValue1.isEmpty()) {
                        throw new IllegalArgumentException("Field " + field + " does not exist in " + o1.getClass().getSimpleName());
                    }
                    Object o1Value = ((Map<?, ?>) fieldValue1.get()).values().iterator().next();
                    Optional<Object> fieldValue2 = ReflectionUtil.getFieldValue(o2, field);
                    if (fieldValue2.isEmpty()) {
                        throw new IllegalArgumentException("Field " + field + " does not exist in " + o2.getClass().getSimpleName());
                    }
                    Object o2Value = ((Map<?, ?>) fieldValue2.get()).values().iterator().next();

                    if (o1Value instanceof Comparable) {
                        return ((Comparable) o1Value).compareTo(o2Value);
                    }

                    if (o1Value instanceof Number) {
                        return Double.compare(((Number) o1Value).doubleValue(), ((Number) o2Value).doubleValue());
                    }

                    return o1Value.toString().compareTo(o2Value.toString());
                }).collect(Collectors.toList());
            } else {
                values = values.stream().sorted((o1, o2) -> {
                    Object o1Value = ((Map<?, ?>) ReflectionUtil.getFieldValue(o1, field).get()).keySet().iterator().next();
                    Object o2Value = ((Map<?, ?>) ReflectionUtil.getFieldValue(o2, field).get()).keySet().iterator().next();

                    if (o1Value instanceof Comparable) {
                        return ((Comparable) o1Value).compareTo(o2Value);
                    }

                    if (o1Value instanceof Number) {
                        return Double.compare(((Number) o1Value).doubleValue(), ((Number) o2Value).doubleValue());
                    }

                    return o1Value.toString().compareTo(o2Value.toString());
                }).collect(Collectors.toList());
            }

            if (this == DESCENDING) {
                Collections.reverse((List<?>) values);
            }

            return values;
        }

        values = values.stream().sorted((o1, o2) -> {
            Object o1Value = ReflectionUtil.getFieldValue(o1, field).get();
            Object o2Value = ReflectionUtil.getFieldValue(o2, field).get();

            if (o1Value instanceof Comparable) {
                return ((Comparable) o1Value).compareTo(o2Value);
            }

            if (o1Value instanceof Number) {
                return Double.compare(((Number) o1Value).doubleValue(), ((Number) o2Value).doubleValue());
            }

            return o1Value.toString().compareTo(o2Value.toString());
        }).collect(Collectors.toList());

        if (this == DESCENDING) {
            Collections.reverse((List<?>) values);
        }

        return values;
    }
}
