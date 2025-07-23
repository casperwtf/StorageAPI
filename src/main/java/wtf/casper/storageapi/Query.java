package wtf.casper.storageapi;

import lombok.Getter;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.List;

@Getter
@Accessors(fluent = true)
public class Query {
    private final List<Condition> conditions = new ArrayList<>();
    private final List<Sort> sorts = new ArrayList<>();
    private final List<Aggregation> aggregations = new ArrayList<>();
    private boolean distinct = false;
    private int limit = -1;
    private int offset = 0;

    public static Query of() {
        return new Query();
    }

    public Query condition(Condition conditions) {
        this.conditions.add(conditions);
        return this;
    }

    public Query sort(Sort sorts) {
        this.sorts.add(sorts);
        return this;
    }

    public Query aggregation(Aggregation aggregations) {
        this.aggregations.add(aggregations);
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
