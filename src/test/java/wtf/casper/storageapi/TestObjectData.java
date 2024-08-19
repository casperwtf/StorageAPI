package wtf.casper.storageapi;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import wtf.casper.storageapi.id.StorageSerialized;

import java.util.HashMap;
import java.util.Map;

@Getter @EqualsAndHashCode @StorageSerialized
public class TestObjectData {
    private final String address;
    private final String employer;
    private final String email;
    private final String phone;
    private final int age;
    private final TestObjectBalance balance;

    private Map<Integer, TestObjectBalance> balanceMap = new HashMap<>(Map.of(
            1, new TestObjectBalance(100, "USD"),
            2, new TestObjectBalance(200, "USD"),
            3, new TestObjectBalance(300, "USD")
    ));

    public TestObjectData(final String address, final String employer, final String email, final String phone, int age, final TestObjectBalance balance) {
        this.address = address;
        this.employer = employer;
        this.email = email;
        this.phone = phone;
        this.age = age;
        this.balance = balance;
    }

    @Override
    public String toString() {
        return "TestObjectData{" +
                "address='" + address + '\'' +
                ", employer='" + employer + '\'' +
                ", email='" + email + '\'' +
                ", phone='" + phone + '\'' +
                ", age=" + age +
                ", balance=" + balance +
                ", balanceMap=" + balanceMap +
                '}';
    }
}
