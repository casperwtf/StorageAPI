package wtf.casper.storageapi.misc;

import com.mongodb.client.model.Filters;
import org.bson.conversions.Bson;
import wtf.casper.storageapi.FilterType;
import wtf.casper.storageapi.StatelessFieldStorage;

import java.util.*;

public interface IMongoStorage {
    default Object convertUUIDtoString(Object object) {
        if (object instanceof UUID) {
            return object.toString();
        }

        if (object instanceof List) {
            List<Object> list = new ArrayList<>();
            for (Object o : (List<?>) object) {
                list.add(convertUUIDtoString(o));
            }
            return list;
        }

        if (object instanceof Map) {
            Map<String, Object> map = new HashMap<>();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) object).entrySet()) {
                map.put(entry.getKey().toString(), convertUUIDtoString(entry.getValue()));
            }
            return map;
        }

        return object;
    }


    default Bson getDocument(FilterType filterType, String field, Object value) {
        value = convertUUIDtoString(value);
        Bson filter;
        switch (filterType) {
            case EQUALS -> filter = Filters.eq(field, value);
            case NOT_EQUALS -> filter = Filters.ne(field, value);
            case GREATER_THAN -> filter = Filters.gt(field, value);
            case LESS_THAN -> filter = Filters.lt(field, value);
            case GREATER_THAN_OR_EQUAL_TO -> filter = Filters.gte(field, value);
            case LESS_THAN_OR_EQUAL_TO -> filter = Filters.lte(field, value);
            case CONTAINS -> filter = Filters.regex(field, value.toString());
            case STARTS_WITH -> filter = Filters.regex(field, "^" + value.toString());
            case ENDS_WITH -> filter = Filters.regex(field, value.toString() + "$");
            case NOT_CONTAINS -> filter = Filters.not(Filters.regex(field, value.toString()));
            case NOT_STARTS_WITH -> filter = Filters.not(Filters.regex(field, "^" + value.toString()));
            case NOT_ENDS_WITH -> filter = Filters.not(Filters.regex(field, value.toString() + "$"));
            case NOT_GREATER_THAN -> filter = Filters.not(Filters.gt(field, value));
            case NOT_LESS_THAN -> filter = Filters.not(Filters.lt(field, value));
            case NOT_GREATER_THAN_OR_EQUAL_TO -> filter = Filters.not(Filters.gte(field, value));
            case NOT_LESS_THAN_OR_EQUAL_TO -> filter = Filters.not(Filters.lte(field, value));
            default -> throw new IllegalStateException("Unexpected value: " + filterType);
        }
        return filter;
    }

}
