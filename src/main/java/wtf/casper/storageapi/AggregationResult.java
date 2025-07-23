package wtf.casper.storageapi;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(fluent = true)
@AllArgsConstructor
public class AggregationResult {
    private String alias;
    private Object value;
}