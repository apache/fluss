package org.apache.fluss.bucketing;

import org.apache.fluss.metadata.DataLakeFormat;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

public class BucketingFunctionTest {
    @Test
    public void testDefaultBucketing() {
        BucketingFunction defaultBucketing = BucketingFunction.of(null);

        assertThat(defaultBucketing.bucketing(new byte[]{(byte) 0, (byte) 10}, 7))
                .isEqualTo(1);
        assertThat(defaultBucketing.bucketing(new byte[]{(byte) 0, (byte) 10, (byte) 10, (byte) 10}, 12))
                .isEqualTo(0);
        assertThat(defaultBucketing.bucketing("2bb87d68-baf9-4e64-90f9-f80910419fa6".getBytes(StandardCharsets.UTF_8), 16))
                .isEqualTo(6);
        assertThat(defaultBucketing.bucketing("The quick brown fox jumps over the lazy dog".getBytes(StandardCharsets.UTF_8), 8))
                .isEqualTo(6);
    }

    @Test
    public void testPaimonBucketing() {
        BucketingFunction paimonBucketing = BucketingFunction.of(DataLakeFormat.PAIMON);

        assertThat(paimonBucketing.bucketing(new byte[]{(byte) 0, (byte) 10}, 7))
                .isEqualTo(1);
        assertThat(paimonBucketing.bucketing(new byte[]{(byte) 0, (byte) 10, (byte) 10, (byte) 10}, 12))
                .isEqualTo(11);
        assertThat(paimonBucketing.bucketing("2bb87d68-baf9-4e64-90f9-f80910419fa6".getBytes(StandardCharsets.UTF_8), 16))
                .isEqualTo(12);
        assertThat(paimonBucketing.bucketing("The quick brown fox jumps over the lazy dog".getBytes(StandardCharsets.UTF_8), 8))
                .isEqualTo(0);
    }

    @Test
    public void testLanceBucketing() {
        BucketingFunction lanceBucketing = BucketingFunction.of(DataLakeFormat.LANCE);

        assertThat(lanceBucketing.bucketing(new byte[]{(byte) 0, (byte) 10}, 7))
                .isEqualTo(1);
        assertThat(lanceBucketing.bucketing(new byte[]{(byte) 0, (byte) 10, (byte) 10, (byte) 10}, 12))
                .isEqualTo(0);
        assertThat(lanceBucketing.bucketing("2bb87d68-baf9-4e64-90f9-f80910419fa6".getBytes(StandardCharsets.UTF_8), 16))
                .isEqualTo(6);
        assertThat(lanceBucketing.bucketing("The quick brown fox jumps over the lazy dog".getBytes(StandardCharsets.UTF_8), 8))
                .isEqualTo(6);
    }

    @Test
    public void testIcebergBucketing() {
        BucketingFunction icebergBucketing = BucketingFunction.of(DataLakeFormat.ICEBERG);

        assertThat(icebergBucketing.bucketing(new byte[]{(byte) 0, (byte) 10}, 7))
                .isEqualTo(3);
        assertThat(icebergBucketing.bucketing(new byte[]{(byte) 0, (byte) 10, (byte) 10, (byte) 10}, 12))
                .isEqualTo(4);
        assertThat(icebergBucketing.bucketing("2bb87d68-baf9-4e64-90f9-f80910419fa6".getBytes(StandardCharsets.UTF_8), 16))
                .isEqualTo(12);
        assertThat(icebergBucketing.bucketing("The quick brown fox jumps over the lazy dog".getBytes(StandardCharsets.UTF_8), 8))
                .isEqualTo(3);
    }
}
