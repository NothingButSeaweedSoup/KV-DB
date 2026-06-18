package protocol;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 二进制协议编解码单元测试。
 */
class ProtocolCodecTest {

    @Test
    void shouldEncodeAndDecodeRequest() {
        byte[] key = "testKey".getBytes();
        byte[] value = "testValue".getBytes();

        ByteBuffer buf = BinaryProtocol.buildRequest(BinaryProtocol.CMD_PUT, 42, key, value);

        BinaryProtocol.RequestFrame frame = BinaryProtocol.parseRequest(buf);
        assertThat(frame.cmd()).isEqualTo(BinaryProtocol.CMD_PUT);
        assertThat(frame.reqId()).isEqualTo(42);
        assertThat(frame.key()).containsExactly(key);
        assertThat(frame.value()).containsExactly(value);
    }

    @Test
    void shouldEncodeAndDecodeRequestWithNullValue() {
        byte[] key = "getKey".getBytes();

        ByteBuffer buf = BinaryProtocol.buildRequest(BinaryProtocol.CMD_GET, 1, key, null);

        BinaryProtocol.RequestFrame frame = BinaryProtocol.parseRequest(buf);
        assertThat(frame.cmd()).isEqualTo(BinaryProtocol.CMD_GET);
        assertThat(frame.reqId()).isEqualTo(1);
        assertThat(frame.key()).containsExactly(key);
        assertThat(frame.value()).isNull();
    }

    @Test
    void shouldEncodeAndDecodeRequestWithNullKeyAndValue() {
        ByteBuffer buf = BinaryProtocol.buildRequest(BinaryProtocol.CMD_PING, 99, null, null);

        BinaryProtocol.RequestFrame frame = BinaryProtocol.parseRequest(buf);
        assertThat(frame.cmd()).isEqualTo(BinaryProtocol.CMD_PING);
        assertThat(frame.reqId()).isEqualTo(99);
        assertThat(frame.key()).isNull();
        assertThat(frame.value()).isNull();
    }

    @Test
    void shouldEncodeAndDecodeResponse() {
        byte[] body = "OK".getBytes();

        ByteBuffer buf = BinaryProtocol.buildResponse(BinaryProtocol.STATUS_OK, 100, body);

        BinaryProtocol.ResponseFrame frame = BinaryProtocol.parseResponse(buf);
        assertThat(frame.status()).isEqualTo(BinaryProtocol.STATUS_OK);
        assertThat(frame.reqId()).isEqualTo(100);
        assertThat(frame.body()).containsExactly(body);
    }

    @Test
    void shouldEncodeAndDecodeResponseWithNullBody() {
        ByteBuffer buf = BinaryProtocol.buildResponse(BinaryProtocol.STATUS_NOT_FOUND, 200, null);

        BinaryProtocol.ResponseFrame frame = BinaryProtocol.parseResponse(buf);
        assertThat(frame.status()).isEqualTo(BinaryProtocol.STATUS_NOT_FOUND);
        assertThat(frame.reqId()).isEqualTo(200);
        assertThat(frame.body()).isNull();
    }

    @Test
    void shouldDetectInvalidMagic() {
        ByteBuffer buf = ByteBuffer.allocate(32);
        buf.putShort((short) 0x1234); // 错误的魔数
        buf.put(BinaryProtocol.VERSION);
        buf.put(BinaryProtocol.CMD_GET);
        buf.putLong(1);
        buf.putInt(3);
        buf.put("key".getBytes());
        buf.putInt(0);
        buf.flip();

        assertThatThrownBy(() -> BinaryProtocol.parseRequest(buf))
                .isInstanceOf(ProtocolException.class)
                .hasMessageContaining("魔数");
    }

    @Test
    void shouldDetectUnsupportedVersion() {
        ByteBuffer buf = ByteBuffer.allocate(32);
        buf.putShort(BinaryProtocol.MAGIC);
        buf.put((byte) 99); // 不支持的版本
        buf.put(BinaryProtocol.CMD_GET);
        buf.putLong(1);
        buf.putInt(0);
        buf.putInt(0);
        buf.flip();

        assertThatThrownBy(() -> BinaryProtocol.parseRequest(buf))
                .isInstanceOf(ProtocolException.class)
                .hasMessageContaining("版本");
    }

    @Test
    void shouldPreserveLargePayload() {
        // 测试大 payload（1KB）
        byte[] key = new byte[256];
        byte[] value = new byte[1024];
        for (int i = 0; i < key.length; i++) key[i] = (byte) (i & 0xFF);
        for (int i = 0; i < value.length; i++) value[i] = (byte) (i & 0xFF);

        ByteBuffer buf = BinaryProtocol.buildRequest(BinaryProtocol.CMD_PUT, 12345, key, value);

        BinaryProtocol.RequestFrame frame = BinaryProtocol.parseRequest(buf);
        assertThat(frame.key()).containsExactly(key);
        assertThat(frame.value()).containsExactly(value);
    }

    @Test
    void requestHeaderSizeShouldBeCorrect() {
        assertThat(BinaryProtocol.REQUEST_HEADER_SIZE).isEqualTo(12);
        assertThat(BinaryProtocol.RESPONSE_HEADER_SIZE).isEqualTo(12);
    }
}
