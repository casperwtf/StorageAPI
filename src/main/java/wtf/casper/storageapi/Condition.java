package wtf.casper.storageapi;

import java.util.ArrayList;
import java.util.List;

public record Condition(String key, Object value, ConditionType conditionType, Type type) {
    public static Condition of(String key, Object value, ConditionType conditionType, Type type) {
        return new Condition(key, value, conditionType, type);
    }

    public static Condition of(String key, Object value, ConditionType conditionType) {
        return new Condition(key, value, conditionType, Type.AND);
    }

    public static Condition and(String key, Object value, ConditionType conditionType) {
        return new Condition(key, value, conditionType, Type.AND);
    }

    public static Condition or(String key, Object value, ConditionType conditionType) {
        return new Condition(key, value, conditionType, Type.OR);
    }

    public enum Type {
        AND,
        OR
    }

    /**
     * Groups filters into a list of lists, where each list is a group of filters that are connected by OR
     * [AND, OR, AND, AND, OR, AND] -> [[AND] OR [AND, AND] OR [AND]]
     * @param conditions the filters to group
     * @return the grouped filters
     */
    public static List<List<Condition>> group(Condition... conditions) {
        List<List<Condition>> groups = new ArrayList<>();
        groups.add(new ArrayList<>());
        for (Condition condition : conditions) {
            List<Condition> lastGroup = groups.get(groups.size() - 1);
            if (condition.type() == Type.AND) {
                lastGroup.add(condition);
                continue;
            }

            if (!lastGroup.isEmpty()) {
                groups.add(new ArrayList<>());
                lastGroup = groups.get(groups.size() - 1);
            }
            lastGroup.add(condition);
        }

        return groups;
    }
}
