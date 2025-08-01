package wtf.casper.storageapi;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(fluent = true)
@AllArgsConstructor
public class Aggregation {
    private AggregateFunction function;
    private String field;
    private String alias;
}
