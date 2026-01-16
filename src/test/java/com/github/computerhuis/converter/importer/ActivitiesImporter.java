package com.github.computerhuis.converter.importer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.computerhuis.converter.jdbi.AccessJdbi;
import com.github.computerhuis.converter.jdbi.MariadbJdbi;
import com.github.computerhuis.converter.util.PathTestUtils;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.List;
import java.util.Map;

@Slf4j
public class ActivitiesImporter extends AbstractImporter {

    public ActivitiesImporter(final AccessJdbi accessJdbi,
                              final MariadbJdbi mariadbJdbi,
                              final ObjectMapper mapper) {
        super(accessJdbi, mariadbJdbi, mapper);
    }

    public void importFromJson() throws Exception {
        log.info("Start importing activities from json");
        val jsonString = PathTestUtils.readFileAsStringFromClasspath("data/activities.json");
        final List<Map<String, Object>> rows = mapper.readValue(jsonString, List.class);
        for (val row : rows) {
            if (!mariadbJdbi.exist("activities", "id", row.get("id"))) {
                mariadbJdbi.insert("activities", row);
            }
        }
    }
}
