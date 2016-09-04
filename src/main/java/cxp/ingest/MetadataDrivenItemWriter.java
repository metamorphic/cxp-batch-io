package cxp.ingest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.LocalDateTime;
import org.springframework.batch.item.ItemWriter;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Timestamp;
import java.util.List;

/**
 * Created by markmo on 7/04/15.
 */
public class MetadataDrivenItemWriter implements ItemWriter<List<CustomerEvent>> {

    private static final Log log = LogFactory.getLog(MetadataDrivenItemWriter.class);

    private static final String INSERT_EVENT_SQL = "INSERT INTO cxp.events (customer_id_type_id, customer_id, event_type_id, event_ts, event_version, value, job_id, process_name, created_ts) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String INSERT_EVENT_PROPERTY_SQL = "INSERT INTO cxp.event_properties (customer_id_type_id, customer_id, event_type_id, event_ts, event_version, property_type_id, version, value) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

    private JdbcTemplate jdbcTemplate;

    private Timestamp created;

    public MetadataDrivenItemWriter() {
        this.created = new Timestamp(LocalDateTime.now().toDateTime().getMillis());
    }

    @Override
    public void write(List<? extends List<CustomerEvent>> items) throws Exception {
        for (List<CustomerEvent> events : items) {
            for (final CustomerEvent event : events) {
                if (log.isDebugEnabled()) {
                    log.debug("inserting event: " + event.getValue() + " for customer: " + event.getCustomerId());
                }
                Timestamp ts;
                if (event.getTs() == null) {
                    ts = created;
                } else {
                    ts = new Timestamp(event.getTs().toDateTime().getMillis());
                }
                jdbcTemplate.update(INSERT_EVENT_SQL,
                        event.getCustomerIdTypeId(), event.getCustomerId(),
                        event.getEventTypeId(), ts, 1, event.getValue().toString(),
                        event.getJobId(), "cxp-ingest-1.0", created);

                if (event.getProperties() != null && !event.getProperties().isEmpty()) {
                    for (CustomerEventProperty property : event.getProperties()) {
                        jdbcTemplate.update(INSERT_EVENT_PROPERTY_SQL,
                                event.getCustomerIdTypeId(), event.getCustomerId(),
                                event.getEventTypeId(), ts, 1,
                                property.getPropertyTypeId(), 1, property.getValue());
                    }
                }
            }
        }
    }

    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }
}