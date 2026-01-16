package com.github.computerhuis.converter.jdbi;

import lombok.NonNull;
import lombok.val;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

abstract class AbstractJdbi {

    protected Jdbi jdbi;
    protected Handle handle;

    public boolean exist(@NonNull final String table, @NonNull final String column, @NonNull final Object value) {
        val sql = "SELECT TRUE AS exist FROM %s WHERE %s=:value".formatted(table, column);
        val found = handle.createQuery(sql).bind("value", value).mapTo(Boolean.class).findOne();
        return found.orElse(false);
    }

    public boolean exist(@NonNull final String table, @NonNull final Map<String, Object> columnValue) {
        val sqlBuilder = new ArrayList<String>();
        for (val entry : columnValue.entrySet()) {
            sqlBuilder.add("%s=:%s".formatted(entry.getKey(), entry.getKey()));
        }

        var sql = "SELECT TRUE AS exist FROM %s WHERE %s".formatted(table, String.join(" AND ", sqlBuilder));
        val handleQuery = handle.createQuery(sql);
        for (val entry : columnValue.entrySet()) {
            handleQuery.bind(entry.getKey(), entry.getValue());
        }
        val found = handleQuery.mapTo(Boolean.class).findOne();
        return found.orElse(false);
    }

    public String create_sql(@NonNull final String table, @NonNull final Map<String, Object> data) {
        return "INSERT INTO %s (%s) VALUES (:%s);".formatted(table, String.join(",", data.keySet()), String.join(",:", data.keySet()));
    }

    public void insert(@NonNull final String table, @NonNull final Map<String, Object> row) {
        val sql = create_sql(table, row);
        handle.createUpdate(sql).bindMap(row).execute();
    }

    public List<Map<String, Object>> select(@NonNull final String sql) {
        return handle.createQuery(sql).mapToMap().list();
    }

    public Handle getHandle() {
        return handle;
    }

    public void open() {
        handle = jdbi.open();
    }

    public void close() {
        handle.close();
    }
}
