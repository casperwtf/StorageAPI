package wtf.casper.storageapi;

import lombok.Getter;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.List;

@Getter
@Accessors(fluent = true)
public class Query {
    private List<Condition> conditions = List.of();
    private List<Sort> sorts = List.of();
    private List<Aggregation> aggregations = List.of();
    private boolean distinct = false;
    private int limit = -1;
    private int offset = 0;

    public static Query of() {
        return new Query();
    }

    public Query condition(Condition... conditions) {
        conditions = conditions == null ? new Condition[0] : conditions;
        this.conditions = new ArrayList<>(List.of(conditions));
        return this;
    }

    public Query sort(Sort... sorts) {
        sorts = sorts == null ? new Sort[0] : sorts;
        this.sorts = new ArrayList<>(List.of(sorts));
        return this;
    }

    public Query aggregation(Aggregation... aggregations) {
        aggregations = aggregations == null ? new Aggregation[0] : aggregations;
        this.aggregations = new ArrayList<>(List.of(aggregations));
        return this;
    }

    public Query distinct(boolean distinct) {
        this.distinct = distinct;
        return this;
    }

    public Query limit(int limit) {
        this.limit = limit;
        return this;
    }

    public Query offset(int offset) {
        this.offset = offset;
        return this;
    }
}
