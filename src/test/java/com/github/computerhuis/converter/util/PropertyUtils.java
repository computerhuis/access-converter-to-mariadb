package com.github.computerhuis.converter.util;

import lombok.extern.slf4j.Slf4j;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

@Slf4j
public final class PropertyUtils extends Properties {

    private static PropertyUtils INSTANCE;

    private PropertyUtils() {
        try (InputStream input = new FileInputStream("config/application.properties")) {
            load(input);
        } catch (Exception io) {
            log.error("Exception: {}", io.getMessage());
            throw new RuntimeException(io);
        }
    }

    public synchronized static Properties getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new PropertyUtils();
        }
        return INSTANCE;
    }
}
