package com.github.computerhuis.converter.jdbi;

import com.github.computerhuis.converter.util.PropertyUtils;
import lombok.Getter;
import lombok.val;
import net.ucanaccess.jdbc.UcanaccessDataSource;
import org.jdbi.v3.core.Jdbi;

@Getter
public class AccessJdbi extends AbstractJdbi {

    private static AccessJdbi instance;

    private AccessJdbi() {
        val properties = PropertyUtils.getInstance();
        val ds = new UcanaccessDataSource();
        ds.setAccessPath(properties.getProperty("datasource.access.path"));
        jdbi = Jdbi.create(ds);
    }

    public static synchronized AccessJdbi getInstance() {
        if (instance == null) {
            instance = new AccessJdbi();
        }
        return instance;
    }
}
