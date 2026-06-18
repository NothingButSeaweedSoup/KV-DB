package raft;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CommandCodecTest {

    // ======================== PUT 编解码 ========================

    @Test
    void encodeAndDecode_putWithStringValue() {
        byte[] key = "hello".getBytes();
        String value = "world";
        Command original = new Command(Command.CommandType.PUT, key, value);

        byte[] encoded = CommandCodec.encode(original);
        Command decoded = CommandCodec.decode(encoded);

        assertThat(decoded.type()).isEqualTo(Command.CommandType.PUT);
        assertThat(decoded.key()).isEqualTo(key);
        assertThat(decoded.value()).isEqualTo("world");
    }

    @Test
    void encodeAndDecode_putWithIntegerValue() {
        byte[] key = "count".getBytes();
        Integer value = 42;
        Command original = new Command(Command.CommandType.PUT, key, value);

        byte[] encoded = CommandCodec.encode(original);
        Command decoded = CommandCodec.decode(encoded);

        assertThat(decoded.type()).isEqualTo(Command.CommandType.PUT);
        assertThat(decoded.key()).isEqualTo(key);
        // Jackson 默认反序列化为 Integer
        assertThat(decoded.value()).isEqualTo(42);
    }

    @Test
    void encodeAndDecode_putWithNullValue() {
        byte[] key = "key1".getBytes();
        Command original = new Command(Command.CommandType.PUT, key, null);

        byte[] encoded = CommandCodec.encode(original);
        Command decoded = CommandCodec.decode(encoded);

        assertThat(decoded.type()).isEqualTo(Command.CommandType.PUT);
        assertThat(decoded.key()).isEqualTo(key);
        assertThat(decoded.value()).isNull();
    }

    @Test
    void encodeAndDecode_putWithEmptyKey() {
        byte[] key = new byte[0];
        String value = "val";
        Command original = new Command(Command.CommandType.PUT, key, value);

        byte[] encoded = CommandCodec.encode(original);
        Command decoded = CommandCodec.decode(encoded);

        assertThat(decoded.type()).isEqualTo(Command.CommandType.PUT);
        assertThat(decoded.key()).isEmpty();
        assertThat(decoded.value()).isEqualTo("val");
    }

    @Test
    void encodeAndDecode_putWithChineseKeyValue() {
        byte[] key = "键".getBytes();
        String value = "值";
        Command original = new Command(Command.CommandType.PUT, key, value);

        byte[] encoded = CommandCodec.encode(original);
        Command decoded = CommandCodec.decode(encoded);

        assertThat(decoded.type()).isEqualTo(Command.CommandType.PUT);
        assertThat(new String(decoded.key())).isEqualTo("键");
        assertThat(decoded.value()).isEqualTo("值");
    }

    // ======================== DELETE 编解码 ========================

    @Test
    void encodeAndDecode_delete() {
        byte[] key = "toDelete".getBytes();
        Command original = new Command(Command.CommandType.DELETE, key, null);

        byte[] encoded = CommandCodec.encode(original);
        Command decoded = CommandCodec.decode(encoded);

        assertThat(decoded.type()).isEqualTo(Command.CommandType.DELETE);
        assertThat(decoded.key()).isEqualTo(key);
        assertThat(decoded.value()).isNull();
    }

    // ======================== FLUSH 编解码 ========================

    @Test
    void encodeAndDecode_flush() {
        Command original = new Command(Command.CommandType.FLUSH, null, null);

        byte[] encoded = CommandCodec.encode(original);
        Command decoded = CommandCodec.decode(encoded);

        assertThat(decoded.type()).isEqualTo(Command.CommandType.FLUSH);
        assertThat(decoded.key()).isNull();
        assertThat(decoded.value()).isNull();
    }

    // ======================== GET 请求编解码 ========================

    @Test
    void encodeAndDecode_getRequest() {
        byte[] key = "myKey".getBytes();

        byte[] encoded = CommandCodec.encodeGetRequest(key);
        byte[] decoded = CommandCodec.decodeGetRequest(encoded);

        assertThat(decoded).isEqualTo(key);
    }

    @Test
    void encodeAndDecode_getRequest_emptyKey() {
        byte[] key = new byte[0];

        byte[] encoded = CommandCodec.encodeGetRequest(key);
        byte[] decoded = CommandCodec.decodeGetRequest(encoded);

        assertThat(decoded).isEmpty();
    }

    // ======================== GET 响应编解码 ========================

    @Test
    void encodeAndDecode_getResponse_withValue() {
        String value = "result";

        byte[] encoded = CommandCodec.encodeGetResponse(value);
        Object decoded = CommandCodec.decodeGetResponse(encoded);

        assertThat(decoded).isEqualTo("result");
    }

    @Test
    void encodeAndDecode_getResponse_withNull() {
        byte[] encoded = CommandCodec.encodeGetResponse(null);
        Object decoded = CommandCodec.decodeGetResponse(encoded);

        assertThat(decoded).isNull();
    }

    @Test
    void encodeAndDecode_getResponse_withInteger() {
        byte[] encoded = CommandCodec.encodeGetResponse(999);
        Object decoded = CommandCodec.decodeGetResponse(encoded);

        assertThat(decoded).isEqualTo(999);
    }

    // ======================== 错误场景 ========================

    @Test
    void decode_invalidTypeCode_throwsException() {
        byte[] invalidData = new byte[]{(byte) 99};

        assertThatThrownBy(() -> CommandCodec.decode(invalidData))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
