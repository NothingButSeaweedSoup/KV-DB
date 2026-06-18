package cluster;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ReplicationCodecTest {

    @Test
    void shouldEncodeAndDecodePutMessage() {
        ReplicationMessage msg = new ReplicationMessage(
                42, ReplicationMessage.Operation.PUT, "hello".getBytes(), "world");

        byte[] encoded = ReplicationCodec.encode(msg);
        ReplicationMessage decoded = ReplicationCodec.decode(encoded);

        assertThat(decoded.getSeq()).isEqualTo(42);
        assertThat(decoded.getOp()).isEqualTo(ReplicationMessage.Operation.PUT);
        assertThat(decoded.getKey()).isEqualTo("hello".getBytes());
        assertThat(decoded.getValue()).isEqualTo("world");
    }

    @Test
    void shouldEncodeAndDecodeDeleteMessage() {
        ReplicationMessage msg = new ReplicationMessage(
                100, ReplicationMessage.Operation.DELETE, "toDelete".getBytes(), null);

        byte[] encoded = ReplicationCodec.encode(msg);
        ReplicationMessage decoded = ReplicationCodec.decode(encoded);

        assertThat(decoded.getSeq()).isEqualTo(100);
        assertThat(decoded.getOp()).isEqualTo(ReplicationMessage.Operation.DELETE);
        assertThat(decoded.getKey()).isEqualTo("toDelete".getBytes());
        assertThat(decoded.getValue()).isNull();
    }

    @Test
    void shouldEncodeAndDecodeBinaryKey() {
        byte[] binaryKey = new byte[256];
        for (int i = 0; i < 256; i++) binaryKey[i] = (byte) i;

        ReplicationMessage msg = new ReplicationMessage(
                1, ReplicationMessage.Operation.PUT, binaryKey, "val");

        byte[] encoded = ReplicationCodec.encode(msg);
        ReplicationMessage decoded = ReplicationCodec.decode(encoded);

        assertThat(decoded.getKey()).isEqualTo(binaryKey);
        assertThat(decoded.getValue()).isEqualTo("val");
    }

    @Test
    void encodedSizeShouldBeSmallerThanJavaSerialization() throws Exception {
        ReplicationMessage msg = new ReplicationMessage(
                999, ReplicationMessage.Operation.PUT, "testKey".getBytes(), "testValue");

        byte[] custom = ReplicationCodec.encode(msg);

        // Java 原生序列化
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        try (java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(baos)) {
            oos.writeObject(msg);
        }
        byte[] javaNative = baos.toByteArray();

        // 自定义序列化应更紧凑
        assertThat(custom.length).isLessThan(javaNative.length);
    }

    @Test
    void shouldHandleEmptyKey() {
        ReplicationMessage msg = new ReplicationMessage(
                1, ReplicationMessage.Operation.PUT, new byte[0], "value");

        byte[] encoded = ReplicationCodec.encode(msg);
        ReplicationMessage decoded = ReplicationCodec.decode(encoded);

        assertThat(decoded.getKey()).isEmpty();
        assertThat(decoded.getValue()).isEqualTo("value");
    }

    @Test
    void shouldPreserveSequenceNumber() {
        for (long seq : new long[]{0, 1, Long.MAX_VALUE, 123456789L}) {
            ReplicationMessage msg = new ReplicationMessage(
                    seq, ReplicationMessage.Operation.PUT, "k".getBytes(), "v");

            byte[] encoded = ReplicationCodec.encode(msg);
            ReplicationMessage decoded = ReplicationCodec.decode(encoded);

            assertThat(decoded.getSeq()).isEqualTo(seq);
        }
    }
}
