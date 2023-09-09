package wtf.casper.storageapi.id.utils;

import lombok.SneakyThrows;
import wtf.casper.storageapi.id.Id;
import wtf.casper.storageapi.id.exceptions.IdNotFoundException;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;

public final class IdUtils {

    @SneakyThrows
    public static Object getId(final Object instance) {
        return getId(instance.getClass(), instance);
    }

    @SneakyThrows
    public static Object getId(final Class<?> clazz, final Object instance) {

        final List<Field> fields = Arrays.stream(clazz.getDeclaredFields())
                .filter(field -> !Modifier.isStatic(field.getModifiers()))
                .toList();

        for (final Field field : fields) {
            field.setAccessible(true);

            if (!field.isAnnotationPresent(Id.class)) {
                continue;
            }

            return field.get(instance);
        }

        final Method method = IdUtils.getIdMethod(clazz);

        return method.invoke(instance);
    }

    @SneakyThrows
    public static String getIdName(final Class<?> type) {

        final List<Field> fields = Arrays.stream(type.getDeclaredFields())
                .filter(field -> !Modifier.isStatic(field.getModifiers()))
                .toList();

        for (final Field field : fields) {
            field.setAccessible(true);

            if (!field.isAnnotationPresent(Id.class)) {
                continue;
            }

            return field.getName();
        }

        final Method method = IdUtils.getIdMethod(type);

        return method.getName();

    }

    @SneakyThrows
    public static Class<?> getIdClass(final Class<?> type) {

        final List<Field> fields = Arrays.stream(type.getDeclaredFields())
                .filter(field -> !Modifier.isStatic(field.getModifiers()))
                .toList();

        for (final Field field : fields) {
            field.setAccessible(true);

            if (!field.isAnnotationPresent(Id.class)) {
                continue;
            }

            return field.getDeclaringClass();
        }

        final Method method = IdUtils.getIdMethod(type);

        return method.getDeclaringClass();

    }

    private static Method getIdMethod(final Class<?> type) throws IdNotFoundException {

        final List<Method> methods = Arrays.stream(type.getDeclaredMethods())
                .filter(method -> !Modifier.isStatic(method.getModifiers()))
                .toList();

        for (final Method method : methods) {
            method.setAccessible(true);

            if (!method.isAnnotationPresent(Id.class)) {
                continue;
            }

            return method;
        }

        throw new IdNotFoundException(type);
    }

    public static Field getIdField(final Class<?> type) throws IdNotFoundException {

        final List<Field> fields = Arrays.stream(type.getDeclaredFields())
                .filter(field -> !Modifier.isStatic(field.getModifiers()))
                .toList();

        for (final Field field : fields) {
            field.setAccessible(true);

            if (!field.isAnnotationPresent(Id.class)) {
                continue;
            }

            return field;
        }

        throw new IdNotFoundException(type);
    }


}
