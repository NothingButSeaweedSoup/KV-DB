package util;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ByteUtilTest {

    @Test
    void shouldCompareEqualArrays() {
        byte[] a = {1, 2, 3};
        byte[] b = {1, 2, 3};
        assertThat(ByteUtil.compare(a, b)).isZero();
    }

    @Test
    void shouldCompareByFirstDifferentByte() {
        byte[] a = {1, 2, 3};
        byte[] b = {1, 2, 4};
        assertThat(ByteUtil.compare(a, b)).isNegative();
        assertThat(ByteUtil.compare(b, a)).isPositive();
    }

    @Test
    void shouldCompareByLengthWhenPrefixEqual() {
        byte[] a = {1, 2, 3};
        byte[] b = {1, 2};
        assertThat(ByteUtil.compare(a, b)).isPositive();
        assertThat(ByteUtil.compare(b, a)).isNegative();
    }

    @Test
    void shouldTreatEmptyArrayAsSmallest() {
        byte[] empty = {};
        byte[] nonEmpty = {1};
        assertThat(ByteUtil.compare(empty, nonEmpty)).isNegative();
        assertThat(ByteUtil.compare(nonEmpty, empty)).isPositive();
    }

    @Test
    void shouldRejectNullInput() {
        byte[] valid = {1};
        assertThatThrownBy(() -> ByteUtil.compare(null, valid))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> ByteUtil.compare(valid, null))
                .isInstanceOf(NullPointerException.class);
    }
}
