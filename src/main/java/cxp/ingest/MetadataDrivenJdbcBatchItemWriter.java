package cxp.ingest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.LocalDateTime;
import org.springframework.batch.item.ItemWriter;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by markmo on 7/04/15.
 */
public class MetadataDrivenJdbcBatchItemWriter implements ItemWriter<List<CustomerEvent>> {

    private static final Log log = LogFactory.getLog(MetadataDrivenJdbcBatchItemWriter.class);

    private static final String INSERT_EVENT_SQL = "INSERT INTO cxp.events (customer_id_type_id, customer_id, event_type_id, event_ts, event_version, event_property, source_key, job_id, created_ts) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String INSERT_TEST_EVENT_SQL = "INSERT INTO cxp.events_test (customer_id_type_id, customer_id, event_type_id, event_ts, event_version, event_property, source_key, job_id, created_ts) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private MetadataProvider metadataProvider;

    private JdbcTemplate jdbcTemplate;

    private Timestamp created;

    private static final String[] charTypes = new String[] { "STRING", "TEXT", "NONE" };

    private static final char quoteChar = '"';

    public MetadataDrivenJdbcBatchItemWriter() {
        this.created = new Timestamp(LocalDateTime.now().toDateTime().getMillis());
    }

    @Override
    public void write(List<? extends List<CustomerEvent>> items) throws Exception {
        final List<Event> events = new ArrayList<Event>();
        for (List<CustomerEvent> customerEvents : items) {
            for (final CustomerEvent event : customerEvents) {
                if (log.isDebugEnabled()) {
                    log.debug("inserting event: " + event.getValue() + " for customer: " + event.getCustomerId());
                }
                Timestamp ts;
                if (event.getTs() == null) {
                    ts = created;
                } else {
                    ts = new Timestamp(event.getTs().toDateTime().getMillis());
                }

                final String properties;
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
                } else {
                    properties = null;
                }

                events.add(new Event(
                        event.getCustomerIdTypeId(),
                        event.getCustomerId(),
                        event.getEventTypeId(),
                        ts,
                        event.getValue().toString(),
                        properties,
                        event.getSourceKey(),
                        event.getJobId()
                ));
            }
        }

        String sql = metadataProvider.isTest() ? INSERT_TEST_EVENT_SQL : INSERT_EVENT_SQL;

        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement ps, int i) throws SQLException {
                        ps.setInt(1, events.get(i).getCustomerIdTypeId());
                        ps.setString(2, events.get(i).getCustomerId());
                        ps.setInt(3, events.get(i).getEventTypeId());
                        ps.setTimestamp(4, events.get(i).getTs());
                        ps.setInt(5, 1);
                        //ps.setString(6, events.get(i).getValue());
                        ps.setString(6, events.get(i).getProperties());
                        ps.setString(7, events.get(i).getSourceKey());
                        ps.setLong(8, events.get(i).getJobId() == null ?0:events.get(i).getJobId());
                        ps.setTimestamp(9, created);
                    }

                    @Override
                    public int getBatchSize() {
                        return events.size();
                    }
                });
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

    public void setMetadataProvider(MetadataProvider metadataProvider) {
        this.metadataProvider = metadataProvider;
    }

    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    private static class Event {

        private Integer customerIdTypeId;
        private String customerId;
        private Integer eventTypeId;
        private Timestamp ts;
        private String value;
        private String properties;
        private String sourceKey;
        private Long jobId;

        Event(Integer customerIdTypeId, String customerId, Integer eventTypeId,
              Timestamp ts, String value, String properties, String sourceKey, Long jobId) {
            this.customerIdTypeId = customerIdTypeId;
            this.customerId = customerId;
            this.eventTypeId = eventTypeId;
            this.ts = ts;
            this.value = value;
            this.properties = properties;
            this.sourceKey = sourceKey;
            this.jobId = jobId;
        }

        public Integer getCustomerIdTypeId() {
            return customerIdTypeId;
        }

        public String getCustomerId() {
            return customerId;
        }

        public Integer getEventTypeId() {
            return eventTypeId;
        }

        public Timestamp getTs() {
            return ts;
        }

        public String getValue() {
            return value;
        }

        public String getProperties() {
            return properties;
        }

        public String getSourceKey() {
            return sourceKey;
        }

        public Long getJobId() {
            return jobId;
        }
    }
}
