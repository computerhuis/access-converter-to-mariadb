package com.github.computerhuis.converter.jdbi;

import com.github.computerhuis.converter.util.PropertyUtils;
import lombok.Getter;
import lombok.val;
import org.jdbi.v3.core.Jdbi;
import org.mariadb.jdbc.MariaDbDataSource;

import java.sql.SQLException;

@Getter
public class MariadbJdbi extends AbstractJdbi {

    private static MariadbJdbi instance;

    private MariadbJdbi() throws SQLException {
        val properties = PropertyUtils.getInstance();
        val ds = new MariaDbDataSource();
        ds.setUrl(properties.getProperty("datasource.mariadb.url"));
        ds.setUser(properties.getProperty("datasource.mariadb.username"));
        ds.setPassword(properties.getProperty("datasource.mariadb.password"));
        jdbi = Jdbi.create(ds);
    }

    public static synchronized MariadbJdbi getInstance() throws SQLException {
        if (instance == null) {
            instance = new MariadbJdbi();
        }
        return instance;
    }
}
