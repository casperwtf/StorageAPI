package wtf.casper.storageapi.misc;

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

}
