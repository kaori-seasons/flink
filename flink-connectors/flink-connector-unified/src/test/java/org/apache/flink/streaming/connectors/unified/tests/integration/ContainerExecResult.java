package org.apache.flink.streaming.connectors.unified.tests.integration;

import lombok.Data;
import org.junit.Assert;

@Data(staticConstructor = "of")
public class ContainerExecResult {

    private final int exitCode;
    private final String stdout;
    private final String stderr;

    public void assertNoOutput() {
        assertNoStdout();
        assertNoStderr();
    }

    public void assertNoStdout() {
        Assert.assertTrue(
                "stdout should be empty, but was '" + stdout + "'",
                stdout.isEmpty());
    }

    public void assertNoStderr() {
        Assert.assertTrue(
                "stderr should be empty, but was '" + stderr + "'",
                stderr.isEmpty());
    }

}
