package org.apache.flink.streaming.connectors.unified.tests.utils;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.command.InspectExecResponse;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.api.model.StreamType;

import org.apache.commons.io.IOUtils;

import org.apache.flink.streaming.connectors.unified.tests.integration.ContainerExecException;
import org.apache.flink.streaming.connectors.unified.tests.integration.ContainerExecResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.zip.GZIPOutputStream;

import static java.nio.charset.StandardCharsets.UTF_8;

public class DockerUtils {
    private static final Logger LOG = LoggerFactory.getLogger(DockerUtils.class);

    private static File getTargetDirectory(String containerId) {
        String base = System.getProperty("maven.buildDirectory");
        if (base == null) {
            base = "target";
        }
        File directory = new File(base + "/container-logs/" + containerId);
        if (!directory.exists() && !directory.mkdirs()) {
            LOG.error("Error creating directory for container logs.");
        }
        return directory;
    }

    public static void dumpContainerLogToTarget(DockerClient dockerClient, String containerId) {
        final String containerName = getContainerName(dockerClient, containerId);
        File output = getUniqueFileInTargetDirectory(containerName, "docker", ".log");
        try (OutputStream os = new BufferedOutputStream(new FileOutputStream(output))) {
            CompletableFuture<Boolean> future = new CompletableFuture<>();
            dockerClient.logContainerCmd(containerName).withStdOut(true)
                    .withStdErr(true).withTimestamps(true).exec(new ResultCallback<Frame>() {
                        @Override
                        public void close() {
                        }

                        @Override
                        public void onStart(Closeable closeable) {
                        }

                        @Override
                        public void onNext(Frame object) {
                            try {
                                os.write(object.getPayload());
                            } catch (IOException e) {
                                onError(e);
                            }
                        }

                        @Override
                        public void onError(Throwable throwable) {
                            future.completeExceptionally(throwable);
                        }

                        @Override
                        public void onComplete() {
                            future.complete(true);
                        }
                    });
            future.get();
        } catch (RuntimeException | ExecutionException | IOException e) {
            LOG.error("Error dumping log for {}", containerName, e);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOG.info("Interrupted dumping log from container {}", containerName, ie);
        }
    }

    private static File getUniqueFileInTargetDirectory(String containerName, String prefix, String suffix) {
        return getUniqueFileInDirectory(getTargetDirectory(containerName), prefix, suffix);
    }

    private static File getUniqueFileInDirectory(File directory, String prefix, String suffix) {
        File file = new File(directory, prefix + suffix);
        int i = 0;
        while (file.exists()) {
            LOG.info("{} exists, incrementing", file);
            file = new File(directory, prefix + "_" + (i++) + suffix);
        }
        return file;
    }

    private static String getContainerName(DockerClient dockerClient, String containerId) {
        final InspectContainerResponse inspectContainerResponse = dockerClient.inspectContainerCmd(containerId).exec();
        // docker api returns names prefixed with "/", it's part of it's legacy design,
        // this removes it to be consistent with what docker ps shows.
        return inspectContainerResponse.getName().replace("/", "");
    }

    public static void dumpContainerDirToTargetCompressed(DockerClient dockerClient, String containerId,
                                                          String path) {
        final String containerName = getContainerName(dockerClient, containerId);
        final String baseName = path.replace("/", "-").replaceAll("^-", "");
        File output = getUniqueFileInTargetDirectory(containerName, baseName, ".tar.gz");
        try (InputStream dockerStream = dockerClient.copyArchiveFromContainerCmd(containerId, path).exec();
             OutputStream os = new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(output)))) {
            IOUtils.copy(dockerStream, os);
        } catch (RuntimeException | IOException e) {
            if (!(e instanceof NotFoundException)) {
                LOG.error("Error reading dir from container {}", containerName, e);
            }
        }
    }


    public static ContainerExecResult runCommand(DockerClient docker,
                                                 String containerId,
                                                 String... cmd)
            throws ContainerExecException, ExecutionException, InterruptedException {
        try {
            return runCommandAsync(docker, containerId, cmd).get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof ContainerExecException) {
                throw (ContainerExecException) e.getCause();
            }
            throw e;
        }
    }

    public static ContainerExecResult runCommandAsUser(String userId,
                                                       DockerClient docker,
                                                       String containerId,
                                                       String... cmd)
            throws ContainerExecException, ExecutionException, InterruptedException {
        try {
            return runCommandAsyncAsUser(userId, docker, containerId, cmd).get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof ContainerExecException) {
                throw (ContainerExecException) e.getCause();
            }
            throw e;
        }
    }

    public static CompletableFuture<ContainerExecResult> runCommandAsyncAsUser(String userId,
                                                                               DockerClient dockerClient,
                                                                               String containerId,
                                                                               String... cmd) {
        String execId = dockerClient.execCreateCmd(containerId)
                .withCmd(cmd)
                .withAttachStderr(true)
                .withAttachStdout(true)
                .withUser(userId)
                .exec()
                .getId();
        return runCommandAsync(execId, dockerClient, containerId, cmd);
    }

    public static CompletableFuture<ContainerExecResult> runCommandAsync(DockerClient dockerClient,
                                                                         String containerId,
                                                                         String... cmd) {
        String execId = dockerClient.execCreateCmd(containerId)
                .withCmd(cmd)
                .withAttachStderr(true)
                .withAttachStdout(true)
                .exec()
                .getId();
        return runCommandAsync(execId, dockerClient, containerId, cmd);
    }

    private static CompletableFuture<ContainerExecResult> runCommandAsync(String execId,
                                                                          DockerClient dockerClient,
                                                                          String containerId,
                                                                          String... cmd) {
        CompletableFuture<ContainerExecResult> future = new CompletableFuture<>();
        final String containerName = getContainerName(dockerClient, containerId);
        String cmdString = String.join(" ", cmd);
        StringBuilder stdout = new StringBuilder();
        StringBuilder stderr = new StringBuilder();
        dockerClient.execStartCmd(execId).withDetach(false)
                .exec(new ResultCallback<Frame>() {
                    @Override
                    public void close() {
                    }

                    @Override
                    public void onStart(Closeable closeable) {
                        LOG.info("DOCKER.exec({}:{}): Executing...", containerName, cmdString);
                    }

                    @Override
                    public void onNext(Frame object) {
                        LOG.info("DOCKER.exec({}:{}): {}", containerName, cmdString, object);
                        if (StreamType.STDOUT == object.getStreamType()) {
                            stdout.append(new String(object.getPayload(), UTF_8));
                        } else if (StreamType.STDERR == object.getStreamType()) {
                            stderr.append(new String(object.getPayload(), UTF_8));
                        }
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        future.completeExceptionally(throwable);
                    }

                    @Override
                    public void onComplete() {
                        LOG.info("DOCKER.exec({}:{}): Done", containerName, cmdString);

                        InspectExecResponse resp = waitForExecCmdToFinish(dockerClient, execId);
                        int retCode = resp.getExitCode();
                        ContainerExecResult result = ContainerExecResult.of(
                                retCode,
                                stdout.toString(),
                                stderr.toString()
                        );
                        LOG.info("DOCKER.exec({}:{}): completed with {}", containerName, cmdString, retCode);

                        if (retCode != 0) {
                            LOG.error(
                                    "DOCKER.exec({}:{}): completed with non zero return code: {}\nstdout: {}\nstderr:"
                                            + " {}",
                                    containerName, cmdString, result.getExitCode(), result.getStdout(),
                                    result.getStderr());
                            future.completeExceptionally(new ContainerExecException(cmdString, containerId, result));
                        } else {
                            future.complete(result);
                        }
                    }
                });
        return future;
    }

    private static InspectExecResponse waitForExecCmdToFinish(DockerClient dockerClient, String execId) {
        InspectExecResponse resp = dockerClient.inspectExecCmd(execId).exec();
        while (resp.isRunning()) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(ie);
            }
            resp = dockerClient.inspectExecCmd(execId).exec();
        }
        return resp;
    }

}
