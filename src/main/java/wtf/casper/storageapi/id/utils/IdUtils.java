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

    public static Object getId(final Object instance) {
        return getId(instance.getClass(), instance);
    }

    public static Object getId(final Class<?> clazz, final Object instance) {

        final List<Field> fields = Arrays.stream(clazz.getDeclaredFields())
                .filter(field -> !Modifier.isStatic(field.getModifiers()))
                .filter(field -> !Modifier.isTransient(field.getModifiers()))
                .toList();

        for (final Field field : fields) {
            field.setAccessible(true);

            if (!field.isAnnotationPresent(Id.class)) {
                continue;
            }

            try {
                return field.get(instance);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        try {
            final Method method = IdUtils.getIdMethod(clazz);
            return method.invoke(instance);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }

    public static String getIdName(final Class<?> type) {

        final List<Field> fields = Arrays.stream(type.getDeclaredFields())
                .filter(field -> !Modifier.isStatic(field.getModifiers()))
                .filter(field -> !Modifier.isTransient(field.getModifiers()))
                .toList();

        for (final Field field : fields) {
            field.setAccessible(true);

            if (!field.isAnnotationPresent(Id.class)) {
                continue;
            }

            return field.getName();
        }

        try {
            final Method method = IdUtils.getIdMethod(type);

            return method.getName();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }

    public static Class<?> getIdClass(final Class<?> type) {

        final List<Field> fields = Arrays.stream(type.getDeclaredFields())
                .filter(field -> !Modifier.isStatic(field.getModifiers()))
                .filter(field -> !Modifier.isTransient(field.getModifiers()))
                .toList();

        for (final Field field : fields) {
            field.setAccessible(true);

            if (!field.isAnnotationPresent(Id.class)) {
                continue;
            }

            return field.getDeclaringClass();
        }

        try {
            final Method method = IdUtils.getIdMethod(type);

            return method.getDeclaringClass();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private static Method getIdMethod(final Class<?> type) throws IdNotFoundException {

        final List<Method> methods = Arrays.stream(type.getDeclaredMethods())
                .filter(method -> !Modifier.isStatic(method.getModifiers()))
                .filter(method -> !Modifier.isTransient(method.getModifiers()))
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
                .filter(field -> !Modifier.isTransient(field.getModifiers()))
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
