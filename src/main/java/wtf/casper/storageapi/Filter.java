package wtf.casper.storageapi;

import java.util.ArrayList;
import java.util.List;

public record Filter(String key, Object value, FilterType filterType, SortingType sortingType, Type type) {
    public static Filter of(String key, Object value, FilterType filterType, SortingType sortingType, Type type) {
        return new Filter(key, value, filterType, sortingType, type);
    }

    public static Filter of(String key, Object value, FilterType filterType, SortingType sortingType) {
        return new Filter(key, value, filterType, sortingType, Type.AND);
    }

    public static Filter of(String key, Object value, FilterType filterType) {
        return new Filter(key, value, filterType, SortingType.NONE, Type.AND);
    }

    public enum Type {
        AND,
        OR
    }

    public static List<List<Filter>> group(Filter... filters) {
        List<List<Filter>> groups = new ArrayList<>();
        groups.add(new ArrayList<>());
        for (Filter filter : filters) {
            if (filter.type() == Type.AND) {
                groups.get(groups.size() - 1).add(filter);
            } else {
                if (!groups.get(groups.size() - 1).isEmpty()) {
                    groups.add(new ArrayList<>());
                }
                groups.get(groups.size() - 1).add(filter);
            }
        }

        return groups;
    }
}
