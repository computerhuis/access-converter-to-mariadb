package com.github.computerhuis.converter.importer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.computerhuis.converter.jdbi.AccessJdbi;
import com.github.computerhuis.converter.jdbi.MariadbJdbi;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class TimesheetImporter extends AbstractImporter {

    public TimesheetImporter(final AccessJdbi accessJdbi,
                             final MariadbJdbi mariadbJdbi,
                             final ObjectMapper mapper) {
        super(accessJdbi, mariadbJdbi, mapper);
    }

    public void importFromAccess(final String importDateFrom, final String importDateTimeFrom) throws Exception {
        log.info("Start importing timesheets from access");
        final List<Map<String, Object>> rows = accessJdbi.select("SELECT * FROM Tbl_Tijdsregistratie WHERE Datum > #%s# and Activiteit is not null".formatted(importDateFrom));
        for (val timesheet : rows) {
            Integer activity_id = lookupActiviteitNr(String.valueOf(timesheet.get("activiteit")));

            LocalDateTime registered = LocalDateTime.parse(importDateTimeFrom);
            if (timesheet.get("datum") != null && timesheet.get("datum") instanceof Timestamp) {
                registered = ((Timestamp) timesheet.get("datum")).toLocalDateTime();
            }

            Integer person_id = (Integer) timesheet.get("gebruikersnummer");

            if (mariadbJdbi.exist("persons", "id", person_id) && !doesTimesheetExist(person_id, activity_id, registered)) {
                Map<String, Object> row = new HashMap<>();
                row.put("person_id", person_id);
                row.put("registered", registered);
                row.put("activity_id", activity_id);
                row.put("unregistered", registered);

                mariadbJdbi.insert("timesheets", row);
            }
        }
    }

    private boolean doesTimesheetExist(final Integer person_id, final Integer activity_id, final LocalDateTime registered) throws Exception {
        val sql = "SELECT TRUE AS exist FROM timesheets WHERE person_id=:person_id AND activity_id=:activity_id AND registered=:registered";
        val found = mariadbJdbi.getHandle().createQuery(sql)
            .bind("person_id", person_id)
            .bind("activity_id", activity_id)
            .bind("registered", registered)
            .mapTo(Boolean.class).findOne();
        return found.orElse(false);
    }

    private Integer lookupActiviteitNr(@NonNull final String activiteit) throws Exception {
        return switch (activiteit.toUpperCase()) {
            case "CURSUSSEN" -> 1;
            case "LESOPMAAT" -> 2;
            case "MEDEWERKER" -> 3;
            case "MEDEWERKER WERKPLAATS" -> 4;
            case "ONLINE BEGELEIDING" -> 5;
            case "TAALONDERSTEUNING" -> 6;
            case "VRIJE INLOOP" -> 7;
            case "WERKPLAATS" -> 8;
            case "WORKSHOPS" -> 9;
            case "VLINDERTUIN" -> 10;
            case "KLIK & TIK", "X" -> 11;
            default -> throw new IllegalStateException("Unexpected value: " + activiteit.toUpperCase());
        };
    }
}
