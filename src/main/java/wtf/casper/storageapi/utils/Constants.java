package wtf.casper.storageapi.utils;

import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.Getter;
import lombok.Setter;
import org.bson.json.JsonWriterSettings;
import org.objenesis.ObjenesisStd;
import wtf.casper.storageapi.id.Transient;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class Constants {
    public static final ObjenesisStd OBJENESIS_STD = new ObjenesisStd(true);
    public final static boolean DEBUG = false;
    private final static AtomicInteger ID = new AtomicInteger(0);
    public final static Executor EXECUTOR = Executors.newFixedThreadPool(Math.max(1, Runtime.getRuntime().availableProcessors() / 4), r -> {
        return new Thread(r, "StorageAPI-Thread-" + ID.incrementAndGet());
    });

    public static void debug(String message) {
        if (DEBUG) {
            System.out.println(message);
        }
    }

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
