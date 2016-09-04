package cxp.ingest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.ItemProcessor;

import java.util.List;
import java.util.Map;

/**
 * Created by markmo on 7/04/15.
 */
public class MetadataDrivenItemProcessor implements ItemProcessor<Map<String, Object>, List<CustomerEvent>> {

    private static final Log log = LogFactory.getLog(MetadataDrivenItemProcessor.class);

    MetadataDrivenItemTransformer transformer;

    @Override
    public List<CustomerEvent> process(Map<String, Object> item) throws Exception {
        return transformer.<Map<String, Object>>transform(item);
    }

    public void setTransformer(MetadataDrivenItemTransformer transformer) {
        this.transformer = transformer;
    }
}
