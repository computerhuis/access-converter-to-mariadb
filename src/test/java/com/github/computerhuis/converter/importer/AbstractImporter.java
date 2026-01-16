package com.github.computerhuis.converter.importer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.computerhuis.converter.jdbi.AccessJdbi;
import com.github.computerhuis.converter.jdbi.MariadbJdbi;
import lombok.AllArgsConstructor;

import static org.apache.commons.lang3.StringUtils.*;

@AllArgsConstructor
public abstract class AbstractImporter {

    protected final AccessJdbi accessJdbi;
    protected final MariadbJdbi mariadbJdbi;
    protected final ObjectMapper mapper;

    protected static String cleanup(final Object value) {

        if (value != null) {
            if (!(value instanceof String)) {
                throw new IllegalArgumentException("value can't be cleaned if its an [%s]".formatted(value.getClass().getName()));
            } else if (isNotBlank((String) value)) {
                return ((String) value).trim();
            }
        }

        return null;
    }

    protected static Integer cleanup_house_number(final String house_number) {
        if (isBlank(house_number)) {
            return null;
        }
        if (isNumeric(house_number)) {
            return Integer.valueOf(house_number);
        }
        return Integer.parseInt(house_number.replaceAll("[^0-9]", ""));
    }

    protected static String cleanup_huisnummer_toevoeging(final String house_number, final String house_number_addition) {
        if (isBlank(house_number_addition)) {
            return null;
        }
        return house_number_addition.replace(house_number, "").trim();
    }
}
