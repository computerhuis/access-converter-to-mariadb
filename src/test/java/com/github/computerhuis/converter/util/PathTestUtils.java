package com.github.computerhuis.converter.util;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class PathTestUtils {

    public static Path getPathFromClasspath(@NonNull final String path) throws FileNotFoundException {
        try {
            return Path.of(PathTestUtils.class.getClassLoader().getResource(path).toURI());
        } catch (URISyntaxException e) {
            throw new FileNotFoundException(e.getMessage());
        }
    }

    public static String readFileAsStringFromClasspath(final String fileName) throws IOException {
        val file = getPathFromClasspath(fileName);
        return Files.readString(file);
    }

    public static byte[] readFileAsByteArrayFromClasspath(final String fileName) throws IOException {
        val file = getPathFromClasspath(fileName);
        return Files.readAllBytes(file);
    }

    public static String readFileAsStringFromTargetDirectory(final String fileName) throws IOException {
        val path = findFileInTargetDirectory(fileName);
        return Files.readString(path);
    }

    public static void writeToFileInTargetDirectory(final String value, final String fileName) throws IOException {
        writeToFileInTargetDirectory(value.getBytes(), fileName);
    }

    public static void writeToFileInTargetDirectory(final byte[] value, final String fileName) throws IOException {
        val path = findFileInTargetDirectory(fileName);
        Files.write(path, value);
    }

    public static Path findFileInTargetDirectory(final String fileName) throws IOException {
        val path = Paths.get("target", fileName);
        if (!Files.exists(path.getParent())) {
            Files.createDirectories(path.getParent());
        }
        if (!Files.exists(path)) {
            Files.createFile(path);
        }
        return path;
    }

    public static void deleteFileInTargetDirectory(final String fileName) throws IOException {
        val path = findFileInTargetDirectory(fileName);
        Files.walk(path)
            .sorted(Comparator.reverseOrder())
            .map(Path::toFile)
            .forEach(File::delete);
    }
}
