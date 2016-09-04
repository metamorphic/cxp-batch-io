package cxp.ingest;

import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by markmo on 26/04/15.
 */
public class MetadataDrivenFlatFilePropertyWriter extends FlatFileItemWriter<List<CustomerEvent>> {

    private String processingFolder;

    private String testProcessingFolder;

    private boolean append = true;

    private MetadataProvider metadataProvider;

    private static final char columnDelimiter = ',';

    private static final String[] charTypes = new String[] { "STRING", "TEXT", "NONE" };

    private static final char quoteChar = '"';

    public void setProcessingFolder(String processingFolder) {
        this.processingFolder = processingFolder;
    }

    public void setTestProcessingFolder(String testProcessingFolder) {
        this.testProcessingFolder = testProcessingFolder;
    }

    @Override
    public void setAppendAllowed(boolean append) {
        this.append = append;
        super.setAppendAllowed(append);
    }

    public void setMetadataProvider(MetadataProvider metadataProvider) {
        this.metadataProvider = metadataProvider;

        String outputDir = metadataProvider.isTest() ? testProcessingFolder : processingFolder;

        Resource output;
        if (append) {
            // Append to the same file
            output = new FileSystemResource(outputDir + "properties.filepart");
        } else {
            String filename = metadataProvider.getFilename();
            String filepart = filename.substring(0, filename.lastIndexOf(".processing"));
            output = new FileSystemResource(outputDir + filepart + "_properties.filepart");
        }

        setResource(output);

        final String lineSeparator = System.getProperty("line.separator");

        setLineAggregator(new LineAggregator<List<CustomerEvent>>() {

            @Override
            public String aggregate(List<CustomerEvent> item) {
                StringBuilder sb = new StringBuilder();
                int n = item.size();
                for (int i = 0; i < n; i++) {
                    CustomerEvent event = item.get(i);
                    List<CustomerEventProperty> properties = event.getProperties();
                    if (properties != null) {
                        int k = properties.size();
                        for (int j = 0; j < k; j++) {
                            CustomerEventProperty property = properties.get(j);
                            sb.append(event.getCustomerIdTypeId())
                                    .append(columnDelimiter).append(event.getCustomerId())
                                    .append(columnDelimiter).append(event.getEventTypeId())
                                    .append(columnDelimiter).append(event.getTs())
                                    .append(columnDelimiter).append(1)
                                    .append(columnDelimiter).append(property.getPropertyTypeId())
                                    .append(columnDelimiter).append(1);
                            if (shouldQuoteValue(property.getValueTypeName())) {
                                sb.append(columnDelimiter).append(quoteChar).append(property.getValue()).append(quoteChar);
                            } else {
                                sb.append(columnDelimiter).append(property.getValue());
                            }
                            if (j < (k - 1)) sb.append(lineSeparator);
                        }
                        if (i < (n - 1)) sb.append(lineSeparator);
                    }
                }
                return sb.toString();
            }
        });
    }

    /**
     * Writes out a string followed by a "new line", where the format of the new
     * line separator is determined by the underlying operating system. If the
     * input is not a String and a converter is available the converter will be
     * applied and then this method recursively called with the result. If the
     * input is an array or collection each value will be written to a separate
     * line (recursively calling this method for each value). If no converter is
     * supplied the input object's toString method will be used.<br>
     *
     * @param items list of items to be written to output stream
     * @throws Exception if the transformer or file output fail,
     * WriterNotOpenException if the writer has not been initialized.
     */
    @Override
    public void write(List<? extends List<CustomerEvent>> items) throws Exception {
        List<List<CustomerEvent>> itemList = new ArrayList<List<CustomerEvent>>();
        for (List<CustomerEvent> events : items) {
            if (!events.isEmpty()) {
                itemList.add(events);
            }
        }
        if (!itemList.isEmpty()) {
            super.write(itemList);
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
}
