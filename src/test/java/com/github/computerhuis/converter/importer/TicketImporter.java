package com.github.computerhuis.converter.importer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.computerhuis.converter.jdbi.AccessJdbi;
import com.github.computerhuis.converter.jdbi.MariadbJdbi;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

@Slf4j
public class TicketImporter extends AbstractImporter {

    private static final Map<String, Integer> LOOKUP_NAME = new HashMap<>() {{
        // Joris Pierre Kleijnen
        put("joris", 1937);
        put("Joris", 1937);

        // Frans van der Meijden
        put("Frans", 1544);
        put("frans", 1544);
        put("Frans/Ali", 1544);
        put("Frans/Jan", 1544);
        put("Frans/Antonie", 1544);
        put("Frans/Ary", 1544);
        put("Frans/Sjef", 1544);

        // Ary Safari - Al Baldawi
        put("Ali", 1648);
        put("Ali & Stephan", 1648);
        put("Ali en Bas", 1648);
        put("Ali/Frans", 1648);
        put("Ali/Jan", 1648);
        put("Ali/Stephan", 1648);
        put("joris en ali", 1648);
        put("Ale", 1648);
        put("adi", 1648);
        put("Ary", 1648);
        put("Ari", 1648);
        put("Ary/Frans", 1648);
        put("Ary/Antonie", 1648);
        put("ary", 1648);
        put("Adi", 1648);

        // Henri Dona
        put("henri", 1804);
        put("Hen ri", 1804);
        put("Henry", 1804);
        put("Henri", 1804);

        // Peter Ruyters
        put("Peter", 740);

        // Sjef Lievens
        put("Sjef", 897);
        put("swjef", 897);
        put("sjef", 897);
        put("Sjef/Frans", 897);
        put("Sjef en Frans", 897);
        put("sjf", 897);

        // Thomas Carpentier
        put("Thomas", 1839);
        put("Thomas & Frans", 1839);
        put("Thomas/Frans", 1839);

        // Jan van der Pol
        put("Jan", 1408);
        put("Jan/Frans", 1408);
        put("JAN/Frans", 1408);
        put("Jan van de Pol", 1408);

        // Tim Voogt
        put("Tim Voogt", 2097);
        put("Tim", 2097);

        // Sander Stumpel
        put("Sander", 2106);
        put("sander", 2106);

        // Willie Voets
        put("Willie/Frans", 2174);
        put("Willie-Frans", 2174);
        put("Wil-Frans", 2174);
        put("Willie", 2174);
        put("Willie Voets", 2174);
        put("willie", 2174);

        // Wil Verberne
        put("Wil", 1284);

        // Antonie Gelderblom
        put("Antonie", 2268);
        put("Antonie/Frans", 2268);
    }};

    private static final Map<String, String> LOOKUP_STATUS = new HashMap<>() {{
        put("KLANT GEBELD", "CUSTOMER_CONTACTED");
        put("IN BEHANDELING", "IN_PROGRESS");
        put("WACHTEND", "OPEN");
        put("KLAAR", "READY");
        put("OPGEHAALD", "CLOSED");
        put("KLANT GEBELD GEREED", "CUSTOMER_INFORMED");
    }};

    private final EquipmentImporter equipmentImporter;
    private final PersonImporter personsImporter;

    public TicketImporter(final AccessJdbi accessJdbi,
                          final MariadbJdbi mariadbJdbi,
                          final ObjectMapper mapper,
                          final EquipmentImporter equipmentImporter,
                          final PersonImporter personsImporter) {
        super(accessJdbi, mariadbJdbi, mapper);
        this.equipmentImporter = equipmentImporter;
        this.personsImporter = personsImporter;
    }

    public void importFromAccess(final String importDateFrom, final String importDateTimeFrom) throws Exception {
        log.info("Start importing tickets from access");
        final List<Map<String, Object>> tickets = accessJdbi.select("SELECT * FROM Tbl_Reparaties_main WHERE [datum inname] > #%s#".formatted(importDateFrom));
        for (val ticket : tickets) {
            if (ticket != null && !ticket.isEmpty() && !mariadbJdbi.exist("tickets", "id", ticket.get("reparatienummer"))) {
                String ticket_type = "REPAIR";
                if (String.valueOf(ticket.get("Probleem")).toLowerCase().contains("uitgifte")) {
                    ticket_type = "ISSUE";
                }

                LocalDateTime registered = LocalDateTime.parse(importDateTimeFrom);
                if (ticket.get("datum inname") != null && ticket.get("datum inname") instanceof Timestamp) {
                    registered = ((Timestamp) ticket.get("datum inname")).toLocalDateTime();
                }

                Map<String, Object> row = new HashMap<>();
                row.put("id", ticket.get("reparatienummer"));
                row.put("ticket_type", ticket_type);
                row.put("registered", registered);
                row.put("equipment_id", ticket.get("computernummer"));

                val problem = cleanup(ticket.get("probleem"));
                if (problem != null) {
                    var len = problem.length();
                    val maxLen = 254;
                    if (len > maxLen) {
                        len = maxLen;
                    }

                    row.put("subject", problem.substring(0, len).trim());
                    if (problem.length() > len) {
                        row.put("description", problem);
                    }
                }

                equipmentImporter.importFromAccess((Integer) ticket.get("computernummer"), importDateFrom, importDateTimeFrom);
                mariadbJdbi.insert("tickets", row);

                Map<String, String> details = new HashMap<>();
                details.put("backup", cleanup(ticket.get("backup")));
                details.put("meegeleverde_accessoires", cleanup(ticket.get("bijgeleverde accessoires")));
                details.put("samenvatting", cleanup(ticket.get("samenvatting reparatie")));
                for (val entry : details.entrySet()) {
                    if (entry.getValue() != null && !"geen".equalsIgnoreCase(entry.getValue())) {
                        mariadbJdbi.insert("ticket_details", Map.of(
                            "ticket_id", row.get("id"),
                            "name", entry.getKey(),
                            "value", entry.getValue()
                        ));
                    }
                }

                importTicketStatusAndLog(ticket, importDateTimeFrom);
                importLogs((Integer) ticket.get("reparatienummer"), importDateTimeFrom);
            }
        }
    }

    private void importTicketStatusAndLog(final Map<String, Object> row, final String importDateTimeFrom) throws Exception {
        if (row != null && !row.isEmpty()) {
            Integer personId = LOOKUP_NAME.get(row.get("aangenomen door"));
            if (personId == null) {
                personId = 1;
            } else {
                personsImporter.importFromAccess(personId, importDateTimeFrom);
            }

            Integer uitgevoerdDoor = LOOKUP_NAME.get(row.get("medewerker"));
            if (uitgevoerdDoor == null) {
                uitgevoerdDoor = 1;
            } else {
                personsImporter.importFromAccess(uitgevoerdDoor, importDateTimeFrom);
            }

            mariadbJdbi.insert("ticket_status", Map.of(
                "ticket_id", row.get("reparatienummer"),
                "date", row.get("datum inname"),
                "volunteer_id", personId,
                "status", "OPEN"
            ));
            var lastUpdated = LocalDateTime.parse(String.valueOf(row.get("datum inname")).substring(0, 10) + "T01:00:00");

            var status = cleanup(row.get("status"));
            if (isNotBlank(status)) {
                status = LOOKUP_STATUS.get(status.toUpperCase());
            }


            // --[ IN_PROGRESS ]----------------------------------------------------------------------------------------
            if ("IN_PROGRESS".equals(status) || "READY".equals(status)) {
                val date = String.valueOf(row.get("datum inname")).substring(0, 10) + "T01:00:00";
                mariadbJdbi.insert("ticket_status", Map.of(
                    "ticket_id", row.get("reparatienummer"),
                    "date", date,
                    "volunteer_id", uitgevoerdDoor,
                    "status", status));
            }

            // --[ READY ]----------------------------------------------------------------------------------------------
            if (row.containsKey("datum opgelost") && row.get("datum opgelost") != null) {
                val date = String.valueOf(row.get("datum opgelost")).substring(0, 10) + "T20:00:00";
                mariadbJdbi.insert("ticket_status", Map.of(
                    "ticket_id", row.get("reparatienummer"),
                    "date", date,
                    "volunteer_id", uitgevoerdDoor,
                    "status", "READY"));

                if (lastUpdated.isBefore(LocalDateTime.parse(date))) {
                    lastUpdated = LocalDateTime.parse(date);
                }
            }

            val isClosed = "CLOSED".equals(status);


            // --[ CUSTOMER_CONTACTED ]---------------------------------------------------------------------------------
            if (!"CUSTOMER_CONTACTED".equals(status)) {
                status = "CUSTOMER_INFORMED";
            }

            if (row.containsKey("datum gebeld1") && row.get("datum gebeld1") != null) {
                val date = String.valueOf(row.get("datum gebeld1")).substring(0, 10) + "T07:00:00";
                if (lastUpdated.isBefore(LocalDateTime.parse(date))) {
                    lastUpdated = LocalDateTime.parse(date);
                }

                mariadbJdbi.insert("ticket_status", Map.of(
                    "ticket_id", row.get("reparatienummer"),
                    "date", date,
                    "volunteer_id", uitgevoerdDoor,
                    "status", status
                ));

                mariadbJdbi.insert("ticket_log", Map.of(
                    "ticket_id", row.get("reparatienummer"),
                    "date", date,
                    "volunteer_id", uitgevoerdDoor,
                    "log", row.get("reactie gebeld1")
                ));
            }

            if (row.containsKey("datum gebeld2") && row.get("datum gebeld2") != null) {
                val date = String.valueOf(row.get("datum gebeld2")).substring(0, 10) + "T12:00:00";
                if (lastUpdated.isBefore(LocalDateTime.parse(date))) {
                    lastUpdated = LocalDateTime.parse(date);
                }

                mariadbJdbi.insert("ticket_status", Map.of(
                    "ticket_id", row.get("reparatienummer"),
                    "date", date,
                    "volunteer_id", uitgevoerdDoor,
                    "status", status
                ));

                mariadbJdbi.insert("ticket_log", Map.of(
                    "ticket_id", row.get("reparatienummer"),
                    "date", date,
                    "volunteer_id", uitgevoerdDoor,
                    "log", row.get("reactie gebeld2")
                ));
            }

            if (row.containsKey("datum gebeld3") && row.get("datum gebeld3") != null) {
                val date = String.valueOf(row.get("datum gebeld3")).substring(0, 10) + "T18:00:00";
                if (lastUpdated.isBefore(LocalDateTime.parse(date))) {
                    lastUpdated = LocalDateTime.parse(date);
                }

                mariadbJdbi.insert("ticket_status", Map.of(
                    "ticket_id", row.get("reparatienummer"),
                    "date", date,
                    "volunteer_id", uitgevoerdDoor,
                    "status", status
                ));

                mariadbJdbi.insert("ticket_log", Map.of(
                    "ticket_id", row.get("reparatienummer"),
                    "date", date,
                    "volunteer_id", uitgevoerdDoor,
                    "log", row.get("reactie gebeld3")
                ));
            }

            // --[ CLOSE ]----------------------------------------------------------------------------------------------
            if (isClosed) {
                lastUpdated = lastUpdated.plusSeconds(1);
                val date = lastUpdated.toString().replace("T", " ");

                mariadbJdbi.insert("ticket_status", Map.of(
                    "ticket_id", row.get("reparatienummer"),
                    "date", date,
                    "volunteer_id", uitgevoerdDoor,
                    "status", "CLOSED"
                ));
            }

            // --[ LOG ]------------------------------------------------------------------------------------------------
            if (uitgevoerdDoor == 1) {
                mariadbJdbi.insert("ticket_log", Map.of(
                    "ticket_id", row.get("reparatienummer"),
                    "volunteer_id", uitgevoerdDoor,
                    "log", "Migratie heeft medewerker op 1 gezet, mederwerker [%s] heeft de reparatie aangenomen, echter is deze persoon onbekend.".formatted(String.valueOf(row.get("medewerker")))
                ));
            }
        }
    }

    private void importLogs(final Integer ticketId, final String importDateTimeFrom) throws Exception {
        if (ticketId != null) {
            final List<Map<String, Object>> logs = accessJdbi.select("SELECT * FROM tbl_reparaties_uitgediept WHERE reparatienummer=%s;".formatted(ticketId));
            if (!logs.isEmpty()) {
                int second = 0;
                for (val logRow : logs) {
                    if (logRow != null && logRow.containsKey("rapport") && logRow.get("rapport") != null) {
                        Integer uitgevoerdDoor = LOOKUP_NAME.get(logRow.get("wie"));
                        if (uitgevoerdDoor == null) {
                            uitgevoerdDoor = 1;
                        } else {
                            personsImporter.importFromAccess(uitgevoerdDoor, importDateTimeFrom);
                        }

                        mariadbJdbi.insert("ticket_log", Map.of(
                            "ticket_id", ticketId,
                            "date", String.valueOf(logRow.get("datum")).substring(0, 10) + " 00:00:0" + second,
                            "volunteer_id", uitgevoerdDoor,
                            "log", logRow.get("rapport")
                        ));
                        second++;
                    }
                }
            }
        }
    }
}
