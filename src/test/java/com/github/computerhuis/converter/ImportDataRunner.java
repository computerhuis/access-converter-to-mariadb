package com.github.computerhuis.converter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.computerhuis.converter.importer.*;
import com.github.computerhuis.converter.jdbi.AccessJdbi;
import com.github.computerhuis.converter.jdbi.MariadbJdbi;
import com.github.computerhuis.converter.util.MapperUtils;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.*;

@Slf4j
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ImportDataRunner {

    private static final String IMPORT_DATE_FROM = "2020-01-01";
    private static final String IMPORT_DATE_TIME_FROM = IMPORT_DATE_FROM + "T00:00:00";
    private static final ObjectMapper mapper = MapperUtils.createJsonMapper();

    private static AccessJdbi accessJdbi;
    private static MariadbJdbi mariadbJdbi;

    @BeforeAll
    public static void beforeAll() throws Exception {
        accessJdbi = AccessJdbi.getInstance();
        mariadbJdbi = MariadbJdbi.getInstance();
    }

    @BeforeEach
    public void beforeEach() throws Exception {
        accessJdbi.open();
        mariadbJdbi.open();
    }

    @AfterEach
    public void afterEach() throws Exception {
        accessJdbi.close();
        mariadbJdbi.close();
    }

    @Test
    @Order(1)
    void import_postal_from_json() throws Exception {
        val postalCodeImporter = new PostalCodeImporter(accessJdbi, mariadbJdbi, mapper);
        postalCodeImporter.importFromJson();
    }

    @Test
    @Order(2)
    void import_activities_from_json() throws Exception {
        val activitiesImporter = new ActivitiesImporter(accessJdbi, mariadbJdbi, mapper);
        activitiesImporter.importFromJson();
    }

    @Test
    @Order(3)
    void import_donors_from_json() throws Exception {
        val postalCodeImporter = new PostalCodeImporter(accessJdbi, mariadbJdbi, mapper);
        val donorImporter = new DonorImporter(accessJdbi, mariadbJdbi, mapper, postalCodeImporter);
        donorImporter.importFromJson();
    }

    @Test
    @Order(4)
    void import_persons_from_json() throws Exception {
        val postalCodeImporter = new PostalCodeImporter(accessJdbi, mariadbJdbi, mapper);
        val personsImporter = new PersonImporter(accessJdbi, mariadbJdbi, postalCodeImporter, mapper);
        personsImporter.importFromJson();
    }

    @Test
    @Order(5)
    void import_equipment_from_access() throws Exception {
        val postalCodeImporter = new PostalCodeImporter(accessJdbi, mariadbJdbi, mapper);
        val personsImporter = new PersonImporter(accessJdbi, mariadbJdbi, postalCodeImporter, mapper);
        val donorImporter = new DonorImporter(accessJdbi, mariadbJdbi, mapper, postalCodeImporter);
        val equipmentImporter = new EquipmentImporter(accessJdbi, mariadbJdbi, mapper, personsImporter, donorImporter);
        equipmentImporter.importFromAccess(IMPORT_DATE_FROM, IMPORT_DATE_TIME_FROM);
    }

    @Test
    @Order(6)
    void import_persons_from_access() throws Exception {
        val postalCodeImporter = new PostalCodeImporter(accessJdbi, mariadbJdbi, mapper);
        val personsImporter = new PersonImporter(accessJdbi, mariadbJdbi, postalCodeImporter, mapper);
        personsImporter.importFromAccess(IMPORT_DATE_FROM, IMPORT_DATE_TIME_FROM);
    }

    @Test
    @Order(7)
    void import_timesheets_from_access() throws Exception {
        val timesheetImporter = new TimesheetImporter(accessJdbi, mariadbJdbi, mapper);
        timesheetImporter.importFromAccess(IMPORT_DATE_FROM, IMPORT_DATE_TIME_FROM);
    }

    @Test
    @Order(8)
    void import_tickets_from_access() throws Exception {
        val postalCodeImporter = new PostalCodeImporter(accessJdbi, mariadbJdbi, mapper);
        val personsImporter = new PersonImporter(accessJdbi, mariadbJdbi, postalCodeImporter, mapper);
        val donorImporter = new DonorImporter(accessJdbi, mariadbJdbi, mapper, postalCodeImporter);
        val equipmentImporter = new EquipmentImporter(accessJdbi, mariadbJdbi, mapper, personsImporter, donorImporter);
        val ticketImporter = new TicketImporter(accessJdbi, mariadbJdbi, mapper, equipmentImporter, personsImporter);
        ticketImporter.importFromAccess(IMPORT_DATE_FROM, IMPORT_DATE_TIME_FROM);
    }

    @Test
    @Order(9)
    void import_proof_of_issues_from_access() throws Exception {
        val postalCodeImporter = new PostalCodeImporter(accessJdbi, mariadbJdbi, mapper);
        val personsImporter = new PersonImporter(accessJdbi, mariadbJdbi, postalCodeImporter, mapper);
        val donorImporter = new DonorImporter(accessJdbi, mariadbJdbi, mapper, postalCodeImporter);
        val equipmentImporter = new EquipmentImporter(accessJdbi, mariadbJdbi, mapper, personsImporter, donorImporter);
        val proofOfIssuesImporter = new ProofOfIssuesImporter(accessJdbi, mariadbJdbi, mapper, personsImporter, equipmentImporter);
        proofOfIssuesImporter.importFromAccess(IMPORT_DATE_FROM, IMPORT_DATE_TIME_FROM);
    }
}
