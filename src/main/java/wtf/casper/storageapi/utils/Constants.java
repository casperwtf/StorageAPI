package wtf.casper.storageapi.utils;

import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.java.Log;
import org.bson.json.JsonWriterSettings;
import org.objenesis.ObjenesisStd;
import wtf.casper.storageapi.id.Transient;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

@Log
public class Constants {
    public static final ObjenesisStd OBJENESIS_STD = new ObjenesisStd(true);

    private static final AtomicInteger THREAD_COUNTER = new AtomicInteger(0);
    public static final Executor DB_THREAD_POOL = Executors.newCachedThreadPool(r -> {
        Thread thread = new Thread(r);
        thread.setDaemon(true);
        thread.setName("StorageAPI-DB-Thread-" + THREAD_COUNTER.incrementAndGet());
        return thread;
    });

    @Getter
    private final static JsonWriterSettings jsonWriterSettings = JsonWriterSettings.builder()
            .int64Converter((value, writer) -> writer.writeNumber(value.toString()))
            .build();

    @Getter
    private final static ExclusionStrategy exclusionStrategy = new ExclusionStrategy() {
        @Override
        public boolean shouldSkipField(FieldAttributes f) {
            return f.getAnnotation(Transient.class) != null;
        }

        @Override
        public boolean shouldSkipClass(Class<?> clazz) {
            return clazz.isAnnotationPresent(Transient.class);
        }
    };

    @Getter
    @Setter
    private static Gson gson;

    static {
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.addSerializationExclusionStrategy(exclusionStrategy);
        gson = gsonBuilder.create();
    }

}
