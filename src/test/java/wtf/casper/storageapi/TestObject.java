package wtf.casper.storageapi;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import wtf.casper.storageapi.id.Id;
import wtf.casper.storageapi.id.StorageSerialized;

import java.util.List;
import java.util.UUID;

@Getter @EqualsAndHashCode @StorageSerialized
public class TestObject {
    @Id
    private final UUID id;
    private String name;
    private int age;
    private TestObjectData data;
    private List<TestObjectData> dataList = List.of(
            new TestObjectData("123 Fake Street", "Walmart", "test@test.com", "123-456-7890", age, new TestObjectBalance(100, "USD")),
            new TestObjectData("456 Fake Street", "Target", "nope@nope.com", "098-765-4321", age, new TestObjectBalance(200, "USD")),
            new TestObjectData("789 Fake Street", "Best Buy", "ttt@ttt.com", "111-222-3333", age, new TestObjectBalance(300, "USD"))
    );
    private TestObjectData[] dataArray = new TestObjectData[] {
            new TestObjectData("123 Fake Street", "Walmart", "test@test.com", "123-456-7890", age, new TestObjectBalance(100, "USD")),
            new TestObjectData("456 Fake Street", "Target", "nope@nope.com", "098-765-4321", age, new TestObjectBalance(200, "USD")),
            new TestObjectData("789 Fake Street", "Best Buy", "ttt@ttt.com", "111-222-3333", age, new TestObjectBalance(300, "USD"))
    };
    private List<TestObjectData> emptyDataList = List.of();
    private TestObjectData[] emptyDataArray = new TestObjectData[] {};

    public TestObject(final UUID id) {
        this.id = id;
    }

    public TestObject(final UUID id, final String name, final int age, final TestObjectData data) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.data = data;
    }

    @Override
    public String toString() {
        return "TestObject{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", age=" + age +
                ", data=" + data +
                '}';
    }
}
