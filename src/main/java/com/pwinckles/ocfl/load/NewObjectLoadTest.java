package com.pwinckles.ocfl.load;

import io.ocfl.api.OcflRepository;
import io.ocfl.api.model.ObjectVersionId;
import io.ocfl.api.model.VersionInfo;
import io.ocfl.core.util.FileUtil;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import org.HdrHistogram.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NewObjectLoadTest {

    private static final Logger log = LoggerFactory.getLogger(NewObjectLoadTest.class);

    private final OcflRepository repo;
    private final long iterations;
    private final long warmupIterations;
    private final int threadCount;
    private final int processingThreadCount;
    private final Map<Long, Integer> fileSpec;

    private final ObjectGenerator objectGenerator;

    public NewObjectLoadTest(
            OcflRepository repo,
            Path tempDir,
            long iterations,
            long warmupIterations,
            int threadCount,
            int processingThreadCount,
            Map<Long, Integer> fileSpec) {
        if (iterations < 1) {
            throw new IllegalArgumentException("Iterations must be 1 or more.");
        }
        if (warmupIterations < 1) {
            throw new IllegalArgumentException("Warmup iterations must be 1 or more.");
        }
        if (threadCount < 1) {
            throw new IllegalArgumentException("Thread count must be 1 or more.");
        }
        if (processingThreadCount < 1) {
            throw new IllegalArgumentException("Processing thread count must be 1 or more.");
        }
        if (fileSpec == null || fileSpec.isEmpty()) {
            throw new IllegalArgumentException("File spec must contain 1 or more files.");
        }

        this.repo = Objects.requireNonNull(repo);
        this.iterations = iterations;
        this.warmupIterations = warmupIterations;
        this.threadCount = threadCount;
        this.processingThreadCount = processingThreadCount;
        this.fileSpec = fileSpec;

        this.objectGenerator = new ObjectGenerator(tempDir);
    }

    public Histogram run() throws InterruptedException {
        log.info("Starting load test");

        var histogram = new Histogram(3);
        var threads = new ArrayList<Thread>(threadCount);
        var phaser = new Phaser(threadCount + 1);

        for (var i = 0; i < threadCount; i++) {
            threads.add(createThread(histogram, phaser));
        }

        startThreads(threads);

        phaser.arriveAndAwaitAdvance();
        histogram.reset();
        phaser.arriveAndAwaitAdvance();

        joinThreads(threads);

        log.info("Load test complete");
        return histogram;
    }

    private Thread createThread(Histogram histogram, Phaser phaser) {
        return new Thread() {
            private final String id = UUID.randomUUID().toString();

            @Override
            public void run() {
                setName(id);
                log.info("Starting thread {}", id);

                Path objectPath = null;

                try {
                    log.info("Generating test object");
                    objectPath = objectGenerator.generate(fileSpec);
                    log.info("Generated test object: {}", objectPath);

                    var versionInfo = new VersionInfo()
                            .setUser("Peter", "pwinckles@example.com")
                            .setMessage("Testing");

                    log.info("Running warmup for {} iterations", warmupIterations);
                    runInner(objectPath, versionInfo, warmupIterations);

                    log.info("Warmup complete. Waiting for other threads to finish.");
                    phaser.arriveAndAwaitAdvance();
                    phaser.arriveAndAwaitAdvance();

                    log.info("Running load test for {} iterations", iterations);
                    runInner(objectPath, versionInfo, iterations);

                    log.info("Completed test in thread {}", id);
                } catch (InterruptedException e) {
                    log.info("Thread interrupted");
                } catch (RuntimeException e) {
                    log.error("Error running test. Thread exiting.", e);
                } finally {
                    if (objectPath != null) {
                        FileUtil.safeDeleteDirectory(objectPath);
                    }
                }
            }

            private void runInner(Path objectPath, VersionInfo versionInfo, long iterations)
                    throws InterruptedException {
                ExecutorService executor;
                if (processingThreadCount > 1) {
                    executor = Executors.newWorkStealingPool(processingThreadCount);
                } else {
                    executor = null;
                }
                var runStart = Instant.now();
                var lastLog = runStart;

                for (int i = 0; i < iterations; i++) {
                    if (Thread.interrupted()) {
                        throw new InterruptedException();
                    }

                    var now = Instant.now();

                    if (Duration.between(runStart, now).toMinutes() >= 1
                            && Duration.between(lastLog, now).toMinutes() >= 1) {
                        lastLog = now;
                        log.info("Thread has created {} objects in {}", i, Duration.between(runStart, now));
                    }

                    var objectId = id + "-" + i;
                    try {
                        var opStart = System.nanoTime();
                        try {
                            if (executor != null) {
                                repo.updateObject(ObjectVersionId.head(objectId), versionInfo, updater -> {
                                    List<? extends Future<?>> futures;
                                    try (var files = Files.find(
                                            objectPath, Integer.MAX_VALUE, (file, attrs) -> attrs.isRegularFile())) {
                                        futures = files.map(file -> executor.submit(() -> {
                                                    var logical = objectPath
                                                            .relativize(file)
                                                            .toString();
                                                    updater.addPath(file, logical);
                                                }))
                                                .toList();
                                    } catch (IOException e) {
                                        throw new UncheckedIOException(e);
                                    }
                                    futures.forEach(future -> {
                                        try {
                                            future.get();
                                        } catch (Exception e) {
                                            throw new RuntimeException("Error adding file to object " + objectId, e);
                                        }
                                    });
                                });
                            } else {
                                repo.putObject(ObjectVersionId.head(objectId), objectPath, versionInfo);
                            }
                        } finally {
                            var end = System.nanoTime();
                            histogram.recordValue(end - opStart);
                        }
                        repo.purgeObject(objectId);
                    } catch (RuntimeException e) {
                        log.error("Exception in thread: {}", id, e);
                    }
                }

                log.info("Run completed in {}", Duration.between(runStart, Instant.now()));
                if (executor != null) {
                    executor.shutdownNow();
                }
            }
        };
    }

    private void startThreads(List<Thread> threads) {
        for (var thread : threads) {
            thread.start();
        }
    }

    private void joinThreads(List<Thread> threads) throws InterruptedException {
        for (var thread : threads) {
            thread.join();
        }
    }
}
