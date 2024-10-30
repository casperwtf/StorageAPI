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
            List<Filter> lastGroup = groups.get(groups.size() - 1);
            if (filter.type() == Type.AND) {
                lastGroup.add(filter);
                continue;
            }

            if (!lastGroup.isEmpty()) {
                groups.add(new ArrayList<>());
                lastGroup = groups.get(groups.size() - 1);
            }
            lastGroup.add(filter);
        }

        return groups;
    }
}
