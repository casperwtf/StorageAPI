package wtf.casper.storageapi;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import wtf.casper.storageapi.id.StorageSerialized;

import java.util.List;

@Getter @EqualsAndHashCode @StorageSerialized
public class TestObjectBalance {
    private final int balance;
    private final String currency;

    public TestObjectBalance(final int balance, final String currency) {
        this.balance = balance;
        this.currency = currency;
    }
    
    private List<String> stringList = List.of("test", "test2", "test3");

    @Override
    public String toString() {
        return "TestObjectBalance{" +
                "balance=" + balance +
                ", currency='" + currency + '\'' +
                ", stringList=" + stringList +
                '}';
    }
}
