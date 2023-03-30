package com.pwinckles.ocfl.load;

import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public final class FileSpec {

    private static final Pattern SIZE_PATTERN = Pattern.compile("(\\d+)(\\p{Alpha}+)");

    private enum Unit {
        B,
        KB,
        MB,
        GB;

        public long toBytes(long size) {
            return switch (this) {
                case B -> size;
                case KB -> size * 1024;
                case MB -> size * 1024 * 1024;
                case GB -> size * 1024 * 1024 * 1024;
            };
        }
    }

    private FileSpec() {}

    public static Map<Long, Integer> convert(Map<String, Integer> files) {
        return files.entrySet().stream()
                .map(entry -> {
                    var sizeStr = entry.getKey();

                    var matcher = SIZE_PATTERN.matcher(sizeStr);
                    if (!matcher.matches()) {
                        throw new IllegalArgumentException("Invalid file size: " + sizeStr);
                    }

                    try {
                        var size = Integer.parseInt(matcher.group(1));
                        var unit = Unit.valueOf(matcher.group(2).toUpperCase());
                        return Map.entry(unit.toBytes(size), entry.getValue());
                    } catch (RuntimeException e) {
                        throw new IllegalArgumentException("Invalid file size: " + sizeStr, e);
                    }
                })
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
