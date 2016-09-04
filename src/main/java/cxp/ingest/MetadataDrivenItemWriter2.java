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
public class MetadataDrivenItemWriter2 implements ItemWriter<List<CustomerEvent>> {

    private static final Log log = LogFactory.getLog(MetadataDrivenItemWriter2.class);

    final String INSERT_EVENT_SQL = "INSERT INTO cxp.events (customer_id_type_id, customer_id, event_type_id, event_ts, event_version, value, job_id, process_name, created_ts) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private JdbcTemplate jdbcTemplate;

    private Timestamp created;

    private static final String[] charTypes = new String[] { "STRING", "TEXT", "NONE" };

    private static final char quoteChar = '"';

    public MetadataDrivenItemWriter2() {
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

                String properties = null;
                if (event.getProperties() != null && !event.getProperties().isEmpty()) {
                    List<CustomerEventProperty> propertyList = event.getProperties();
                    int n = propertyList.size();
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < n; i++) {
                        CustomerEventProperty property = propertyList.get(i);
                        if (shouldQuoteValue(property.getValueTypeName())) {
                            sb.append(quoteChar).append(property.getValue()).append(quoteChar);
                        } else {
                            sb.append(property.getValue());
                        }
                        if (i < (n - 1)) sb.append(',');
                    }
                    properties = sb.toString();
                }

                jdbcTemplate.update(INSERT_EVENT_SQL,
                        event.getCustomerIdTypeId(), event.getCustomerId(),
                        event.getEventTypeId(), ts, 1,
                        properties,
                        event.getJobId(), "cxp-ingest-1.0", created);
            }
        }
    }

    private static boolean shouldQuoteValue(String valueTypeName) {
        if (valueTypeName == null) return true;
        for (String type : charTypes) {
            if (type.equals(valueTypeName)) {
                return true;
            }
        }
        return false;
    }

    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }
}
