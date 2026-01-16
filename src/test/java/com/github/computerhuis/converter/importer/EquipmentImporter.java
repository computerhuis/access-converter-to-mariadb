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
public class EquipmentImporter extends AbstractImporter {

    private final PersonImporter personsImporter;
    private final DonorImporter donorImporter;

    public EquipmentImporter(final AccessJdbi accessJdbi,
                             final MariadbJdbi mariadbJdbi,
                             final ObjectMapper mapper,
                             final PersonImporter personsImporter,
                             final DonorImporter donorImporter) {
        super(accessJdbi, mariadbJdbi, mapper);
        this.personsImporter = personsImporter;
        this.donorImporter = donorImporter;
    }

    public void importFromAccess(final String importDateFrom, final String importDateTimeFrom) throws Exception {
        log.info("Start importing equipment from access");
        final List<Map<String, Object>> equipment = accessJdbi.select("SELECT Tbl_computers.* FROM Tbl_computers WHERE [Datum Gift] > #%s#".formatted(importDateFrom));
        for (val item : equipment) {
            importFromAccess(item, importDateTimeFrom, importDateTimeFrom);
        }
    }

    public void importFromAccess(final Integer computerId, final String importDateFrom, final String importDateTimeFrom) throws Exception {
        log.info("Start importing individual {} from access", computerId);
        final List<Map<String, Object>> equipment = accessJdbi.select("SELECT Tbl_computers.* FROM Tbl_computers WHERE Computernummer=%s".formatted(computerId));
        for (val item : equipment) {
            importFromAccess(item, importDateTimeFrom, importDateTimeFrom);
        }
    }

    private void importFromAccess(final Map<String, Object> computer, final String importDateFrom, final String importDateTimeFrom) throws Exception {
        if (computer != null && !computer.isEmpty() && !mariadbJdbi.exist("equipment", "id", computer.get("computernummer"))) {
            val orgcategory = cleanup(computer.get("type kast")).toUpperCase();
            String category = orgcategory;
            if (category == null) {
                category = "DESKTOP";
            } else {
                switch (category) {
                    case "LAPTOP":
                    case "LEERGELD LAPTOP":
                    case "LAPTUP":
                    case "NOTEBOOK":
                        category = "LAPTOP";
                        break;
                    case "MOBILE":
                    case "MOBIEL":
                    case "SMARTPHONE":
                    case "LEERGELD SMARTPHONE":
                        category = "MOBILE";
                        break;
                    case "SIM":
                    case "SIMKAART":
                    case "LEERGELD SIMKAART":
                        category = "SIM";
                        break;
                    case "TABLET":
                        category = "TABLET";
                        break;
                    case "USB STICK":
                        category = "USB_STICK";
                        break;
                    case "MINI TOWER":
                    case "TOWER":
                    case "DESKTOP":
                    case "NULL":
                    case "GEEN":
                    case "":
                        category = "DESKTOP";
                        break;
                    default:
                        throw new RuntimeException("Unsupported type: " + category);
                }
            }
            String status = cleanup(computer.get("status"));
            if (status != null) {
                status = switch (status.toUpperCase()) {
                    case "KLANT PC" -> "CUSTOMER_OWNED";
                    case "BINNENGEKOMEN GIFT" -> "INCOMING_GIFT";
                    case "GESCHIKT VOOR VERKOOP", "KLAAR VOOR VERKOOP" -> "SUITABLE_FOR_GIFT";
                    case "GERESERVEERD" -> "RESERVED";
                    case "VERKOCHT" -> "SOLD";
                    case "SLOOP" -> "DEMOLITION";
                    default -> throw new RuntimeException("Unsupported type: " + status);
                };
            }

            LocalDateTime registered = LocalDateTime.parse(importDateTimeFrom);
            if (computer.get("datum gift") != null && computer.get("datum gift") instanceof Timestamp) {
                registered = ((Timestamp) computer.get("datum gift")).toLocalDateTime();
            }

            Map<String, Object> row = new HashMap<>();
            row.put("category", category);
            row.put("id", computer.get("computernummer"));
            row.put("manufacturer", cleanup(computer.get("fabrikant")));
            row.put("model", cleanup(computer.get("model nummer")));
            row.put("owner_id", computer.get("gebruikersnummer"));
            row.put("status", status);
            row.put("registered", registered);

            val donor_id = (Integer) computer.get("gift van");
            row.put("donor_id", donor_id);
            if (orgcategory.startsWith("LEERGELD")) {
                row.put("behalf_of_id", donor_id);
            }

            Map<String, String> specificatie = new HashMap<>();
            val optische_apparaten = cleanup(computer.get("optische apparaten"));
            if (isNotBlank(optische_apparaten) && !optische_apparaten.trim().equalsIgnoreCase("0")) {
                if (donor_id != null && donor_id == 61) {
                    row.put("serial_number", optische_apparaten.trim());
                } else {
                    specificatie.put("optisch", optische_apparaten.trim());
                }
            }

            val bijzonderheden = cleanup(computer.get("bijzonderheden"));
            if (bijzonderheden != null) {
                if (donor_id != null && donor_id == 61) {

                } else {
                    specificatie.put("bijzonderheden", bijzonderheden);
                }
            }

            specificatie.put("processor", cleanup(computer.get("processor")));
            specificatie.put("geheugen", cleanup(computer.get("geheugen")));
            specificatie.put("harddisk", cleanup(computer.get("hdd ssd")));
            specificatie.put("overige", cleanup(computer.get("overige ingebouwde apparaten")));
            specificatie.put("software", cleanup(computer.get("software")));
            importFromAccess(row, specificatie, importDateTimeFrom);
        }
    }

    private void importFromAccess(final Map<String, Object> equipment, final Map<String, String> specificatie, final String importDateTimeFrom) throws Exception {
        if (equipment != null && !equipment.isEmpty() && !mariadbJdbi.exist("equipment", "id", equipment.get("id"))) {
            log.info("Insert: {}", equipment);
            if (equipment.get("owner_id") != null) {
                personsImporter.importFromAccess((Integer) equipment.get("owner_id"), importDateTimeFrom);
            }
            if (equipment.get("donor_id") != null) {
                donorImporter.importFromAccess((Integer) equipment.get("donor_id"), importDateTimeFrom);
            }
            mariadbJdbi.insert("equipment", equipment);

            for (val entry : specificatie.entrySet()) {
                if (entry.getValue() != null && !"geen".equalsIgnoreCase(entry.getValue())) {
                    mariadbJdbi.insert("equipment_specification", Map.of(
                        "equipment_id", equipment.get("id"),
                        "name", entry.getKey(),
                        "value", entry.getValue()
                    ));
                }
            }
        }
    }
}
