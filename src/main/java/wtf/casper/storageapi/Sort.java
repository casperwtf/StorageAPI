package wtf.casper.storageapi;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(fluent = true)
public class Sort {
    private final String field;
    private final SortingType sortingType;
}
