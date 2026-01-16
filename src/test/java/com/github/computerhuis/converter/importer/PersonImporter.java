package com.github.computerhuis.converter.importer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.computerhuis.converter.jdbi.AccessJdbi;
import com.github.computerhuis.converter.jdbi.MariadbJdbi;
import com.github.computerhuis.converter.util.CsvWriter;
import com.github.computerhuis.converter.util.PathTestUtils;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

@Slf4j
public class PersonImporter extends AbstractImporter {

    private final PostalCodeImporter postalCodeImporter;
    private final CsvWriter writerError;
    private final CsvWriter writerAutoChanges;

    public PersonImporter(final AccessJdbi accessJdbi,
                          final MariadbJdbi mariadbJdbi,
                          final PostalCodeImporter postalCodeImporter,
                          final ObjectMapper mapper) throws IOException {
        super(accessJdbi, mariadbJdbi, mapper);
        this.postalCodeImporter = postalCodeImporter;
        writerError = new CsvWriter("./audits/persons-error.cvs", "id", "voornaam", "achternaam");
        writerAutoChanges = new CsvWriter("./audits/persons-auto-correct.cvs", "id", "voornaam", "achternaam");
    }

    public void importFromJson() throws IOException {
        log.info("Start importing persons from json");
        val jsonString = PathTestUtils.readFileAsStringFromClasspath("data/persons.json");

        final List<Map<String, Object>> rows = mapper.readValue(jsonString, List.class);
        for (val row : rows) {
            if (!mariadbJdbi.exist("persons", "id", row.get("id"))) {
                mariadbJdbi.insert("persons", row);
            }
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // ACCESS
    // -----------------------------------------------------------------------------------------------------------------
    public void importFromAccess(final String importDateFrom, final String importDateTimeFrom) throws Exception {
        log.info("Start importing persons from access");
        final List<Map<String, Object>> rows = accessJdbi.select("SELECT Tbl_Gebruikers_NAW.* FROM Tbl_Gebruikers_NAW WHERE [Datum Inschrijving] > #%s#".formatted(importDateFrom));
        for (val row : rows) {
            importFromAccess(row, importDateTimeFrom);
        }
    }

    public void importFromAccess(final Integer gebruikersnummer, final String importDateTimeFrom) throws Exception {
        log.info("Start importing individual {} from access", gebruikersnummer);
        final List<Map<String, Object>> rows = accessJdbi.select("SELECT Tbl_Gebruikers_NAW.* FROM Tbl_Gebruikers_NAW WHERE Gebruikersnummer=%s".formatted(gebruikersnummer));
        for (val row : rows) {
            importFromAccess(row, importDateTimeFrom);
        }
    }

    private void importFromAccess(final Map<String, Object> individual, final String importDateTimeFrom) throws Exception {
        if (individual != null && !individual.isEmpty() && !mariadbJdbi.exist("persons", "id", individual.get("gebruikersnummer"))) {
            Integer huisnummer = cleanup_house_number((String) individual.get("huisnummer"));
            String huisnummertoevoeging = cleanup_huisnummer_toevoeging(String.valueOf(huisnummer), (String) individual.get("huisnummer"));

            String mobile = null;
            String telefoon = null;

            if (isNotBlank((String) individual.get("1e telefoon")) && ((String) individual.get("1e telefoon")).trim().startsWith("06")) {
                mobile = cleanup(individual.get("1e telefoon"));
            } else {
                telefoon = cleanup(individual.get("1e telefoon"));
            }

            if (isNotBlank((String) individual.get("2e telefoon")) && ((String) individual.get("2e telefoon")).trim().startsWith("06")) {
                mobile = cleanup(individual.get("2e telefoon"));
            } else {
                telefoon = cleanup(individual.get("2e telefoon"));
            }

            LocalDateTime registered = LocalDateTime.parse(importDateTimeFrom);
            if (individual.get("datum inschrijving") != null && individual.get("datum inschrijving") instanceof Timestamp) {
                registered = ((Timestamp) individual.get("datum inschrijving")).toLocalDateTime();
            }

            String postal_code = cleanup(individual.get("postcode"));
            if (postal_code != null) {
                postal_code = postal_code.replace(" ", "").toUpperCase();
            }

            String email = cleanup(individual.get("e-mailadres"));
            if (email != null) {
                email = email.toLowerCase();
            }

            Map<String, Object> row = new HashMap<>();
            row.put("id", individual.get("gebruikersnummer"));
            row.put("initials", cleanup(individual.get("voorletters")));
            row.put("first_name", cleanup(individual.get("voornaam")));
            row.put("infix", cleanup(individual.get("tussenvoegsels")));
            row.put("last_name", cleanup(individual.get("achternaam")));
            row.put("date_of_birth", individual.get("geboortedatum"));
            row.put("email", email);
            row.put("mobile", mobile);
            row.put("telephone", telefoon);
            row.put("postal_code", postal_code);
            row.put("street", cleanup(individual.get("adres")));
            row.put("house_number", huisnummer);
            row.put("house_number_addition", huisnummertoevoeging);
            row.put("city", cleanup(individual.get("plaatsnaam")));
            row.put("registered", registered);
            row.put("comments", cleanup(individual.get("opmerkingen")));
            row.put("msaccess", mapper.writeValueAsString(individual));

            if (!postalCodeImporter.doesPostalCodeExist(postal_code, huisnummer)) {
                log.info("Incorrect address for: person[{}]", individual.get("gebruikersnummer"));
                writerError.write(List.of(individual.get("gebruikersnummer"), individual.get("voornaam"), individual.get("achternaam")));
                var comments = cleanup(row.get("comments"));
                if (individual.get("postal_code") != null) {
                    comments += "\nOrginele postcode: [%s]".formatted(individual.get("postal_code"));
                }
                if (individual.get("adres") != null) {
                    comments += "\nOrginele straatnaam: [%s]".formatted(individual.get("adres"));
                }
                if (individual.get("house_number") != null) {
                    comments += "\nOrginele huisnummer: [%s]".formatted(individual.get("house_number"));
                }
                if (individual.get("house_number_addition") != null) {
                    comments += "\nOrginele huisnummer toevoeging: [%s]".formatted(individual.get("house_number_addition"));
                }
                if (isNotBlank(comments)) {
                    row.put("comments", comments.trim());
                }

                row.remove("postal_code");
                row.remove("street");
                row.remove("house_number");
                row.remove("house_number_addition");
            } else {
                val street = postalCodeImporter.getStreetNamePostalCodeExist(postal_code, huisnummer);
                if (isNotBlank(street) && !((String) individual.get("adres")).equalsIgnoreCase(street)) {
                    row.put("street", street);
                    val comments = row.get("comments") + "\nOrginele straatnaam: [%s]".formatted(individual.get("adres"));
                    row.put("comments", comments.trim());
                    writerAutoChanges.write(List.of(individual.get("gebruikersnummer"), individual.get("voornaam"), individual.get("achternaam")));
                }
            }
            mariadbJdbi.insert("persons", row);

            val bedrijfsnummer = (Integer) individual.get("bedrijfsnummer");
            if (bedrijfsnummer != null && (bedrijfsnummer == 6 || bedrijfsnummer == 1)) {
                Map<String, Object> volunteer_row = new HashMap<>();
                volunteer_row.put("person_id", individual.get("gebruikersnummer"));
                volunteer_row.put("authority", "ROLE_VOLUNTEER");
                mariadbJdbi.insert("person_roles", volunteer_row);
            }
        }
    }
}
