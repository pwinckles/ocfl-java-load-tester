package com.pwinckles.ocfl.load;

import edu.wisc.library.ocfl.api.OcflRepository;
import edu.wisc.library.ocfl.core.util.FileUtil;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

@CommandLine.Command(
        name = "new-obj-test",
        description =
                """
                Generates a test object with characteristics that meet a supplied specification, \
                and then writes that object to random IDs in the OCFL repository as many times as possible.

                When S3 options are specified, the OCFL repository is created in S3. Otherwise, \
                it's created in a directory on the local filesystem.""",
        mixinStandardHelpOptions = true)
public class NewObjectLoadTestCmd implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(NewObjectLoadTestCmd.class);

    @CommandLine.Option(
            names = "--iterations",
            description = "The number of times to run the test in each thread.",
            required = true)
    private long iterations;

    @CommandLine.Option(
            names = "--warmup",
            description = "The number of times to run the test as a warmup.",
            required = true)
    private long warmupIterations;

    @CommandLine.Option(names = "--threads", description = "The number of threads to run the test on.", required = true)
    private int threadCount;

    @CommandLine.Option(
            names = "--files",
            description = "A list of file size and file count pairs that describe the test object composition."
                    + " For example, '10MB=2' means 2 10MB files. Valid units are B, KB, MB, and GB.",
            required = true)
    private Map<String, Integer> files;

    @CommandLine.Option(
            names = "--temp",
            description = "The directory to use to write files before they're inserted into the repository.",
            required = true)
    private Path tempDir;

    @CommandLine.ArgGroup
    private StorageOptions storageOptions;

    static class StorageOptions {
        @CommandLine.Option(
                names = "--dir",
                description = "The path to the directory to create the OCFL repository in.")
        private Path directory;

        @CommandLine.ArgGroup(exclusive = false)
        private S3Options s3Options;

        @Override
        public String toString() {
            return "StorageOptions{" + "directory=" + directory + ", s3Options=" + s3Options + '}';
        }
    }

    static class S3Options {
        @CommandLine.Option(names = "--s3-region", description = "The AWS region.", required = true)
        private String s3Region;

        @CommandLine.Option(
                names = "--s3-bucket",
                description = "The name of the S3 bucket to write to.",
                required = true)
        private String s3Bucket;

        @CommandLine.Option(
                names = "--s3-prefix",
                description = "The prefix within the bucket to create the OCFL repo in.")
        private String s3Prefix;

        @CommandLine.Option(
                names = "--s3-profile",
                description = "The name of the profile to load the credentials from.",
                defaultValue = "default",
                showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
        private String s3Profile;

        @CommandLine.Option(
                names = "--s3-endpoint",
                description = "The S3 endpoint. This only needs to be specified when using a non-AWS endpoint.")
        private String s3Endpoint;

        @Override
        public String toString() {
            return "S3Options{" + "s3Region='"
                    + s3Region + '\'' + ", s3Bucket='"
                    + s3Bucket + '\'' + ", s3Prefix='"
                    + s3Prefix + '\'' + ", s3Profile='"
                    + s3Profile + '\'' + ", s3Endpoint='"
                    + s3Endpoint + '\'' + '}';
        }
    }

    @Override
    public void run() {
        log.info("Running load test with config: {}", this);

        OcflRepository repo;

        try {
            Files.createDirectories(tempDir);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        }

        if (storageOptions.directory != null) {
            repo = RepoBuilder.buildFsRepo(storageOptions.directory);
        } else {
            repo = RepoBuilder.buildS3Repo(
                    storageOptions.s3Options.s3Profile,
                    storageOptions.s3Options.s3Region,
                    storageOptions.s3Options.s3Endpoint,
                    storageOptions.s3Options.s3Bucket,
                    storageOptions.s3Options.s3Prefix,
                    tempDir);
        }

        var fileSpec = FileSpec.convert(files);

        var loadTest = new NewObjectLoadTest(repo, tempDir, iterations, warmupIterations, threadCount, fileSpec);

        try {
            var histogram = loadTest.run();

            cleanup();

            var out = new ByteArrayOutputStream();
            histogram.outputPercentileDistribution(new PrintStream(out), 1_000_000.0);
            log.info("Output in milliseconds:\n{}", out.toString(StandardCharsets.UTF_8));
        } catch (InterruptedException e) {
            throw new RuntimeException("Load test was interrupted.", e);
        }
    }

    private void cleanup() {
        if (storageOptions.directory != null) {
            FileUtil.safeDeleteDirectory(storageOptions.directory.resolve(RepoBuilder.ROOT));
            FileUtil.safeDeleteDirectory(storageOptions.directory.resolve(RepoBuilder.WORK));
        } else {
            FileUtil.safeDeleteDirectory(tempDir.resolve(RepoBuilder.WORK));
        }
    }

    @Override
    public String toString() {
        return "NewObjectLoadTestCmd{" + "iterations="
                + iterations + ", warmupIterations="
                + warmupIterations + ", threadCount="
                + threadCount + ", files="
                + files + ", tempDir="
                + tempDir + ", storageOptions="
                + storageOptions + '}';
    }
}
