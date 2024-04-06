package com.pwinckles.ocfl.load;

import io.ocfl.api.OcflRepository;
import io.ocfl.aws.OcflS3Client;
import io.ocfl.core.OcflRepositoryBuilder;
import io.ocfl.core.cache.NoOpCache;
import io.ocfl.core.extension.storage.layout.config.HashedNTupleLayoutConfig;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

public final class RepoBuilder {

    public static final String ROOT = "ocfl-root";
    public static final String WORK = "ocfl-work";

    private RepoBuilder() {}

    public static OcflRepository buildFsRepo(Path directory) {
        try {
            var root = Files.createDirectories(directory.resolve(ROOT));
            var work = Files.createDirectories(directory.resolve(WORK));
            return new OcflRepositoryBuilder()
                    .defaultLayoutConfig(new HashedNTupleLayoutConfig())
                    .inventoryCache(new NoOpCache<>())
                    .storage(storage -> storage.fileSystem(root))
                    .workDir(work)
                    .build();
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        }
    }

    public static OcflRepository buildS3Repo(
            String profile, String region, String endpoint, String bucket, String prefix, Path tempDir) {
        var clientBuilder = S3Client.builder()
//                .credentialsProvider(ProfileCredentialsProvider.builder()
//                        .profileName(profile)
//                        .build())
                .region(Region.of(region));
        //        var clientBuilder = S3AsyncClient.crtBuilder()
        //                .credentialsProvider(ProfileCredentialsProvider.builder()
        //                        .profileName(profile)
        //                        .build())
        //                .region(Region.of(region));

        if (endpoint != null) {
            clientBuilder.endpointOverride(URI.create(endpoint));
        }

        try {
            return new OcflRepositoryBuilder()
                    .defaultLayoutConfig(new HashedNTupleLayoutConfig())
                    .storage(storage -> storage.cloud(OcflS3Client.builder()
                            .s3Client(clientBuilder.build())
                            .bucket(bucket)
                            .repoPrefix(prefix)
                            .build()))
                    .workDir(Files.createDirectories(tempDir.resolve(WORK)))
                    .build();
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        }
    }
}
