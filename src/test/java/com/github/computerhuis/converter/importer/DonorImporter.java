package com.github.computerhuis.converter.importer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.computerhuis.converter.jdbi.AccessJdbi;
import com.github.computerhuis.converter.jdbi.MariadbJdbi;
import com.github.computerhuis.converter.util.CsvWriter;
import com.github.computerhuis.converter.util.PathTestUtils;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

@Slf4j
public class DonorImporter extends AbstractImporter {

    private final PostalCodeImporter postalCodeImporter;
    private final CsvWriter writerError;
    private final CsvWriter writerAutoChanges;

    public DonorImporter(final AccessJdbi accessJdbi,
                         final MariadbJdbi mariadbJdbi,
                         final ObjectMapper mapper,
                         final PostalCodeImporter postalCodeImporter) throws IOException {
        super(accessJdbi, mariadbJdbi, mapper);
        this.postalCodeImporter = postalCodeImporter;
        writerError = new CsvWriter("./audits/donors-error.cvs", "id", "bedrijfsnaam");
        writerAutoChanges = new CsvWriter("./audits/donors-auto-correct.cvs", "id", "bedrijfsnaam");
    }

    public void importFromJson() throws Exception {
        log.info("Start importing donors from json");
        val jsonString = PathTestUtils.readFileAsStringFromClasspath("data/donors.json");
        final List<Map<String, Object>> rows = mapper.readValue(jsonString, List.class);
        for (val row : rows) {
            if (!mariadbJdbi.exist("donors", "id", row.get("id"))) {
                mariadbJdbi.insert("donors", row);
            }
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // ACCESS
    // -----------------------------------------------------------------------------------------------------------------
    public void importFromAccess(final Integer donorId, final String importDateTimeFrom) throws Exception {
        log.info("Start importing donor {} from access", donorId);
        final List<Map<String, Object>> rows = accessJdbi.select("SELECT Tbl_Bedrijven_NAW.* FROM Tbl_Bedrijven_NAW WHERE BedrijfsNummer=%s".formatted(donorId));
        for (val row : rows) {
            importFromAccess(row, importDateTimeFrom);
        }
    }

    private void importFromAccess(final Map<String, Object> donor, final String importDateTimeFrom) throws Exception {
        if (donor != null && !donor.isEmpty() && !mariadbJdbi.exist("donors", "id", donor.get("bedrijfsnummer"))) {
            log.info("Importing donor {}", donor.get("bedrijfsnummer"));

            String email = cleanup(donor.get("e-mail"));
            if (email != null) {
                email = email.toLowerCase();
            }

            String mobile = null;
            String telefoon = null;
            if (isNotBlank((String) donor.get("telefoon")) && ((String) donor.get("telefoon")).trim().startsWith("06")) {
                mobile = cleanup(donor.get("telefoon"));
            } else {
                telefoon = cleanup(donor.get("telefoon"));
            }

            String postal_code = cleanup(donor.get("postcode"));
            if (postal_code != null) {
                postal_code = postal_code.replace(" ", "").toUpperCase();
            }

            Integer huisnummer = cleanup_house_number((String) donor.get("huisnummer"));
            String huisnummertoevoeging = cleanup_huisnummer_toevoeging(String.valueOf(huisnummer), (String) donor.get("huisnummer"));

            LocalDateTime registered = LocalDateTime.parse(importDateTimeFrom);
            Map<String, Object> row = new HashMap<>();
            row.put("id", donor.get("bedrijfsnummer"));
            row.put("name", cleanup(donor.get("bedrijfsnaam")));
            row.put("email", email);
            row.put("mobile", mobile);
            row.put("telephone", telefoon);
            row.put("postal_code", postal_code);
            row.put("street", cleanup(donor.get("straat")));
            row.put("house_number", huisnummer);
            row.put("house_number_addition", huisnummertoevoeging);
            row.put("city", cleanup(donor.get("plaats")));
            row.put("registered", registered);
            row.put("msaccess", mapper.writeValueAsString(row));

            if (postal_code != null && huisnummer != null) {
                if (!postalCodeImporter.doesPostalCodeExist(postal_code, huisnummer)) {
                    log.info("Incorrect address for: donor[{}]", donor.get("bedrijfsnummer"));
                    writerError.write(List.of(donor.get("bedrijfsnummer"), donor.get("bedrijfsnaam")));
                    var comments = cleanup(row.get("comments"));
                    if (donor.get("postal_code") != null) {
                        comments += "\nOrginele postcode: [%s]".formatted(donor.get("postal_code"));
                    }
                    if (donor.get("straat") != null) {
                        comments += "\nOrginele straatnaam: [%s]".formatted(donor.get("straat"));
                    }
                    if (donor.get("house_number") != null) {
                        comments += "\nOrginele huisnummer: [%s]".formatted(donor.get("house_number"));
                    }
                    if (donor.get("house_number_addition") != null) {
                        comments += "\nOrginele huisnummer toevoeging: [%s]".formatted(donor.get("house_number_addition"));
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
                    if (isNotBlank(street) && !((String) donor.get("straat")).equalsIgnoreCase(street)) {
                        row.put("street", street);
                        val comments = row.get("comments") + "\nOrginele straatnaam: [%s]".formatted(donor.get("straat"));
                        row.put("comments", comments.trim());
                        writerAutoChanges.write(List.of(donor.get("bedrijfsnummer"), donor.get("bedrijfsnaam")));
                    }
                }
            }

            mariadbJdbi.insert("donors", row);
        }
    }
}
