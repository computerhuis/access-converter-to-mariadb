package com.github.computerhuis.converter.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.val;

import java.util.TimeZone;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MapperUtils {

    public static XmlMapper createXmlMapper() {
        return createXmlMapper(false, false);
    }

    public static XmlMapper createXmlMapper(final boolean ignoreUnknownFields, final boolean hideEmptyFields) {
        val mapper = new XmlMapper();
        setupMapper(mapper, ignoreUnknownFields, hideEmptyFields);
        return mapper;
    }

    public static ObjectMapper createJsonMapper() {
        return createJsonMapper(false, false);
    }

    public static ObjectMapper createJsonMapper(final boolean ignoreUnknownFields, final boolean hideEmptyFields) {
        val mapper = new ObjectMapper();
        setupMapper(mapper, ignoreUnknownFields, hideEmptyFields);
        return mapper;
    }

    private static void setupMapper(@NonNull final ObjectMapper mapper, final boolean ignoreUnknownFields, final boolean hideEmptyFields) {
        mapper.registerModule(new JavaTimeModule());
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        if (hideEmptyFields) {
            mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
            mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        }
        mapper.setTimeZone(TimeZone.getDefault());
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, !ignoreUnknownFields);
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
    }
}
