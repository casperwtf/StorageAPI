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

    /**
     * Groups filters into a list of lists, where each list is a group of filters that are connected by OR
     * [AND, OR, AND, AND, OR, AND] -> [[AND] OR [AND, AND] OR [AND]]
     * @param filters the filters to group
     * @return the grouped filters
     */
    public static List<List<Filter>> group(Filter... filters) {
        List<List<Filter>> groups = new ArrayList<>();
        groups.add(new ArrayList<>());
        for (Filter filter : filters) {
            if (filter.type() == Type.AND) {
                groups.get(groups.size() - 1).add(filter);
                continue;
            }

            if (!groups.get(groups.size() - 1).isEmpty()) {
                groups.add(new ArrayList<>());
            }
            groups.get(groups.size() - 1).add(filter);
        }

        return groups;
    }
}
