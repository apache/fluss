package com.alibaba.fluss.fs.s3;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.fs.FsPath;
import com.alibaba.fluss.fs.s3.token.S3DelegationTokenProvider;
import com.alibaba.fluss.fs.token.ObtainedSecurityToken;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link S3FileSystemPlugin}. */
public class S3FileSystemPluginTest {

    private static final URI fsUri =
            new FsPath("s3://test-bucket/tests-" + UUID.randomUUID()).toUri();

    private S3FileSystemPlugin s3FileSystemPlugin;

    @BeforeEach
    void beforeEach() {
        s3FileSystemPlugin = new S3FileSystemPlugin();
    }

    @Test
    void testDeactivateTokenDelegationProcess() throws IOException {
        // Deactivate token delegation process when user does not specify credential provider
        Configuration config = new Configuration();
        config.set(ConfigOptions.FILE_SYSTEM_S3_ENABLE_TOKEN_DELEGATION, false);
        FileSystem fileSystem = s3FileSystemPlugin.create(fsUri, config);
        ObtainedSecurityToken obtainedSecurityToken = fileSystem.obtainSecurityToken();
        assertThat(obtainedSecurityToken).isNotNull();
        assertThat(obtainedSecurityToken.getToken()).isNotNull();
        assertThat(obtainedSecurityToken.getToken().length).isEqualTo(0);
        assertThat(obtainedSecurityToken.getScheme()).isEqualTo("s3");
        assertThat(obtainedSecurityToken.getValidUntil()).isEmpty();
        assertThat(fileSystem).isInstanceOf(S3FileSystem.class);
        assertThat(((S3FileSystem) fileSystem).s3DelegationTokenProvider.getType())
                .isEqualTo(S3DelegationTokenProvider.Type.NO_TOKEN);
    }
}
