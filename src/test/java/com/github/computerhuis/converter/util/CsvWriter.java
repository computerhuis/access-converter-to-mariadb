package com.github.computerhuis.converter.util;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.csv.CSVPrinter;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import static com.github.computerhuis.converter.util.PathTestUtils.deleteFileInTargetDirectory;
import static com.github.computerhuis.converter.util.PathTestUtils.findFileInTargetDirectory;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE;
import static org.apache.commons.csv.CSVFormat.EXCEL;
import static org.apache.commons.csv.QuoteMode.MINIMAL;

@Slf4j
public final class CsvWriter implements AutoCloseable {

    private static final String CSV_FIELD_SEPARATOR = ";";

    private final BufferedWriter writer;
    private final CSVPrinter printer;

    public CsvWriter(@NonNull final String filename, @NonNull final String... header) throws IOException {
        deleteFileInTargetDirectory(filename);
        val path = findFileInTargetDirectory(filename);
        writer = Files.newBufferedWriter(path, UTF_8, CREATE, CREATE);
        // Write BOM character for Excel
        writer.write('\ufeff');
        printer = new CSVPrinter(writer, EXCEL.builder()
            .setDelimiter(CSV_FIELD_SEPARATOR)
            .setQuoteMode(MINIMAL)
            .setHeader(header)
            .get());
    }

    public void write(@NonNull final List<Object> values) throws IOException {
        log.info("Write record: {}", values);
        printer.printRecord(values);
        printer.flush();
    }

    @Override
    public void close() throws Exception {
        if (printer != null) {
            printer.flush();
        }
        if (writer != null) {
            writer.close();
        }
    }
}
