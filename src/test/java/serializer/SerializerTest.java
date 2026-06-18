package serializer;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SerializerTest {

    // ========== BytesSerializer ==========

    @Test
    void bytesSerializerRoundTrip() {
        BytesSerializer s = BytesSerializer.INSTANCE;
        byte[] data = {0x01, 0x02, 0x03, (byte) 0xFF};
        assertThat(s.deserialize(s.serialize(data))).containsExactly(data);
    }

    @Test
    void bytesSerializerRejectsNull() {
        BytesSerializer s = BytesSerializer.INSTANCE;
        assertThatThrownBy(() -> s.serialize(null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> s.deserialize(null)).isInstanceOf(IllegalArgumentException.class);
    }

    // ========== StringSerializer ==========

    @Test
    void stringSerializerRoundTrip() {
        StringSerializer s = StringSerializer.INSTANCE;
        String value = "你好世界Hello";
        assertThat(s.deserialize(s.serialize(value))).isEqualTo(value);
    }

    @Test
    void stringSerializerEmptyString() {
        StringSerializer s = StringSerializer.INSTANCE;
        assertThat(s.deserialize(s.serialize(""))).isEmpty();
    }

    @Test
    void stringSerializerRejectsNull() {
        StringSerializer s = StringSerializer.INSTANCE;
        assertThatThrownBy(() -> s.serialize(null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> s.deserialize(null)).isInstanceOf(IllegalArgumentException.class);
    }

    // ========== JsonSerializer ==========

    @Test
    void jsonSerializerRoundTrip() throws IOException {
        JsonSerializer<SimplePojo> s = new JsonSerializer<>(SimplePojo.class);
        SimplePojo pojo = new SimplePojo("hello", 42);
        SimplePojo result = s.deserialize(s.serialize(pojo));
        assertThat(result.name).isEqualTo("hello");
        assertThat(result.value).isEqualTo(42);
    }

    @Test
    void jsonSerializerWithCollections() throws IOException {
        JsonSerializer<ListWrapper> s = new JsonSerializer<>(ListWrapper.class);
        ListWrapper wrapper = new ListWrapper();
        wrapper.items = List.of("a", "b", "c");
        ListWrapper result = s.deserialize(s.serialize(wrapper));
        assertThat(result.items).containsExactly("a", "b", "c");
    }

    @Test
    void jsonSerializerRejectsNull() {
        JsonSerializer<String> s = new JsonSerializer<>(String.class);
        assertThatThrownBy(() -> s.serialize(null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> s.deserialize(null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void jsonSerializerRejectsNullType() {
        assertThatThrownBy(() -> new JsonSerializer<>(null)).isInstanceOf(IllegalArgumentException.class);
    }

    // ========== JavaSerializer ==========

    @Test
    void javaSerializerRoundTrip() throws IOException {
        JavaSerializer<String> s = new JavaSerializer<>();
        String value = "test value";
        assertThat(s.deserialize(s.serialize(value))).isEqualTo(value);
    }

    @Test
    void javaSerializerWithSerializableObject() throws IOException {
        JavaSerializer<SimpleSerializable> s = new JavaSerializer<>();
        SimpleSerializable obj = new SimpleSerializable(100, "data");
        SimpleSerializable result = s.deserialize(s.serialize(obj));
        assertThat(result.id).isEqualTo(100);
        assertThat(result.data).isEqualTo("data");
    }

    @Test
    void javaSerializerReturnsNullForEmptyBytes() throws IOException {
        JavaSerializer<String> s = new JavaSerializer<>();
        assertThat(s.deserialize(new byte[0])).isNull();
        assertThat(s.deserialize(null)).isNull();
    }

    @Test
    void javaSerializerRejectsNullOnSerialize() {
        JavaSerializer<String> s = new JavaSerializer<>();
        assertThatThrownBy(() -> s.serialize(null)).isInstanceOf(IllegalArgumentException.class);
    }

    // ========== 测试辅助类 ==========

    public static class SimplePojo {
        public String name;
        public int value;

        public SimplePojo() {}

        public SimplePojo(String name, int value) {
            this.name = name;
            this.value = value;
        }
    }

    public static class ListWrapper {
        public List<String> items;
    }

    public static class SimpleSerializable implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        public int id;
        public String data;

        public SimpleSerializable(int id, String data) {
            this.id = id;
            this.data = data;
        }
    }
}
