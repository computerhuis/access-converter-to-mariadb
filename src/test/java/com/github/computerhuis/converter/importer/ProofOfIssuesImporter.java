package com.github.computerhuis.converter.importer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.computerhuis.converter.jdbi.AccessJdbi;
import com.github.computerhuis.converter.jdbi.MariadbJdbi;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class ProofOfIssuesImporter extends AbstractImporter {

    private final Long DEFAULT_ISSUED_BY = 897L; // Sjef Lievens
    private static final Map<Integer, Integer> IGNORE_COMPUTERS = new HashMap<>() {{
        put(2225, 1952);
    }};

    private final PersonImporter personsImporter;
    private final EquipmentImporter equipmentImporter;

    public ProofOfIssuesImporter(final AccessJdbi accessJdbi,
                                 final MariadbJdbi mariadbJdbi,
                                 final ObjectMapper mapper,
                                 final PersonImporter personsImporter,
                                 final EquipmentImporter equipmentImporter) {
        super(accessJdbi, mariadbJdbi, mapper);
        this.personsImporter = personsImporter;
        this.equipmentImporter = equipmentImporter;
    }

    public void importFromAccess(final String importDateFrom, final String importDateTimeFrom) throws Exception {
        log.info("Start importing proof_of_issues from access");
        final List<Map<String, Object>> proofOfIssues = accessJdbi.select("SELECT Tbl_factuur.* FROM Tbl_factuur WHERE [datum] > #%s#".formatted(importDateFrom));
        for (val proofOfIssue : proofOfIssues) {
            importFromAccess(proofOfIssue, importDateTimeFrom, importDateTimeFrom);
        }
    }

    private void importFromAccess(final Map<String, Object> proofOfIssue, final String importDateFrom, final String importDateTimeFrom) throws Exception {
        if (proofOfIssue != null && !proofOfIssue.isEmpty() && !mariadbJdbi.exist("proof_of_issues", "id", proofOfIssue.get("factuurnummer"))) {
            log.info("Importing invoice {}", proofOfIssue.get("factuurnummer"));
            final List<Map<String, Object>> lines = accessJdbi.select("SELECT Tbl_factuur_omschrijvingen.* FROM Tbl_factuur_omschrijvingen WHERE factuurnummer=%s".formatted(proofOfIssue.get("factuurnummer")));
            if (lines == null || lines.isEmpty()) {
                log.warn("No lines found for invoice: {}", proofOfIssue.get("factuurnummer"));
                return;
            }

            log.info("proof_of_issues: {}", proofOfIssue);
            personsImporter.importFromAccess((Integer) proofOfIssue.get("klantnr"), importDateTimeFrom);

            final List<Map<String, Object>> rowLines = new ArrayList<>();
            Long foundBehalfOfId = null;
            Integer foundComputerId = null;
            var index = 1;
            for (val line : lines) {
                log.info("          lines: {}", lines);

                val computerId = (Integer) line.get("computernummer");

                if (IGNORE_COMPUTERS.containsKey(proofOfIssue.get("factuurnummer")) && IGNORE_COMPUTERS.get(proofOfIssue.get("factuurnummer")).equals(computerId)) {
                    continue;
                }

                if (foundComputerId == null) {
                    foundComputerId = computerId;
                }

                equipmentImporter.importFromAccess(computerId, importDateFrom, importDateTimeFrom);
                val equipment = mariadbJdbi.select("SELECT * FROM equipment WHERE id=%s".formatted(computerId));
                if (equipment.isEmpty()) {
                    log.warn("No equipment found for computerId: {}", computerId);
                    return;
                }

                if (equipment.get(0).containsKey("behalf_of_id")) {
                    val behalfOfId = (Long) equipment.get(0).get("behalf_of_id");
                    if (foundBehalfOfId == null) {
                        foundBehalfOfId = behalfOfId;
                    }
                }

                if (foundComputerId != null && !foundComputerId.equals(computerId)) {
                    throw new IllegalArgumentException("computerId is different for equipment with id: " + lines);
                }

                Map<String, Object> rowLine = new HashMap<>();
                rowLine.put("proof_of_issue_id", proofOfIssue.get("factuurnummer"));
                rowLine.put("line_id", index);
                rowLine.put("description", cleanup(line.get("produkt omschrijving")));
                rowLines.add(rowLine);
                index++;

                if (foundComputerId == null) {
                    throw new IllegalArgumentException("computerId is different for equipment with id: " + lines);
                }
            }

            final Map<String, Object> row = new HashMap<>();
            row.put("id", proofOfIssue.get("factuurnummer"));
            row.put("issue_date", proofOfIssue.get("datum"));
            row.put("issued_by", DEFAULT_ISSUED_BY);
            if (foundBehalfOfId != null) {
                row.put("behalf_of_id", foundBehalfOfId);
            }
            row.put("recipient_id", proofOfIssue.get("klantnr"));
            row.put("equipment_id", foundComputerId);

            mariadbJdbi.insert("proof_of_issues", row);
            for (val rowLine : rowLines) {
                mariadbJdbi.insert("proof_of_issue_lines", rowLine);
            }
        }
    }
}
