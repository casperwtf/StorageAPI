package wtf.casper.storageapi;

import lombok.Data;

@Data
public class Sort {
    private final String field;
    private final SortingType sortingType;
}
