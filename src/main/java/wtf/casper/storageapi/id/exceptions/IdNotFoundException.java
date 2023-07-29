package wtf.casper.storageapi.id.exceptions;

public class IdNotFoundException extends Exception {

    public IdNotFoundException(Class<?> type) {
        super("Field or method annotated with @Id not found! Type: " + type.getSimpleName());
    }
}
