package wtf.casper.storageapi;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Aggregation {
    private AggregateFunction function;
    private String field;
    private String alias;
    private Condition filter;
    
    public Aggregation(AggregateFunction fn, String field, String alias) {
        this(fn, field, alias, null);
    }

    public Aggregation withFilter(Condition filter) {
        this.filter = filter;
        return this;
    }
}
