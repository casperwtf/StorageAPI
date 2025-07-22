package wtf.casper.storageapi;

import lombok.Builder;

import java.util.ArrayList;
import java.util.List;

@Builder
public class Query {
    private final List<Condition> conditions;
    private final List<Sort> sorts;
    private final boolean distinct;
    private final int limit;
    private final int offset;

}
