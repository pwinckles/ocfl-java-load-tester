package com.pwinckles.ocfl.load;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class ObjectGenerator {

    private static final int BUFFER_SIZE = 8192;

    private final Path root;

    public ObjectGenerator(Path root) {
        this.root = Objects.requireNonNull(root);
    }

    public Path generate(Map<Long, Integer> fileSpec) {
        var uuid = UUID.randomUUID().toString();
        Path objectPath;

        try {
            objectPath = Files.createDirectories(root.resolve(uuid));
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        }

        int i = 1;

        for (var entry : fileSpec.entrySet()) {
            var size = entry.getKey();
            var count = entry.getValue();
            for (int j = 0; j < count; j++) {
                writeFile(objectPath.resolve("file-" + i++ + ".bin"), size);
            }
        }

        return objectPath;
    }

    private void writeFile(Path path, long size) {
        var bytes = new byte[BUFFER_SIZE];
        try (var out = new BufferedOutputStream(Files.newOutputStream(path))) {
            var written = 0;
            while (written < size) {
                ThreadLocalRandom.current().nextBytes(bytes);
                int len = (int) Math.min(BUFFER_SIZE, size - written);
                out.write(bytes, 0, len);
                written += len;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        }
    }
}
