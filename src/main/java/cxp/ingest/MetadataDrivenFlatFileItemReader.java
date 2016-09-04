package cxp.ingest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.validation.BindException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by markmo on 7/04/15.
 */
public class MetadataDrivenFlatFileItemReader extends FlatFileItemScanner<Map<String, Object>> {

    private static final Log log = LogFactory.getLog(MetadataDrivenFlatFileItemReader.class);

    private static final String TEST_PATTERN = "/test/";

    MetadataProvider metadataProvider;

    public void setMetadataProvider(MetadataProvider metadataProvider) {
        this.metadataProvider = metadataProvider;
    }

    @Override
    public void setResource(Resource resource) {
        super.setResource(resource);

        metadataProvider.setFilename(resource.getFilename());

        final FileDataset fileDataset = metadataProvider.getFileDataset();

        if (fileDataset == null) {
            String message = "No dataset found for '" + resource.getFilename() + "'";
            log.warn(message);
            throw new RuntimeException(message);
        }

        if (log.isDebugEnabled()) {
            log.debug("Found dataset " + fileDataset.getName());
        }

        try {
            Pattern p = Pattern.compile(TEST_PATTERN);
            String absolutePath = resource.getFile().getAbsolutePath();
            Matcher matcher = p.matcher(absolutePath);
            metadataProvider.setTest(matcher.find());
            metadataProvider.startJob();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }

        String rowDelimiter = fileDataset.getRowDelimiter();
        setRowDelimiter(rowDelimiter);

        if (log.isDebugEnabled()) {
            log.debug("row delimiter [" + rowDelimiter + "]");
        }

        if (fileDataset.isHeaderRow()) {
            setLinesToSkip(1);
        }

        final char quotechar;
        String textQualifier = fileDataset.getTextQualifier();
        if (textQualifier == null) {
            quotechar = ',';
        } else {
            quotechar = textQualifier.charAt(0);
        }

        Assert.notNull(fileDataset.getColumnNames());

        setLineMapper(new DefaultLineMapper<Map<String, Object>>() {{

            setLineTokenizer(new MetadataDrivenDelimitedLineTokenizer(fileDataset) {{
                setNames(fileDataset.getColumnNames());
                setQuoteCharacter(quotechar);
                setStrict(false);
            }});

            setFieldSetMapper(new FieldSetMapper<Map<String, Object>>() {
                @Override
                public Map<String, Object> mapFieldSet(FieldSet fieldSet) throws BindException {
                    if (fieldSet == null) return null;
                    Map<String, Object> fields = new HashMap<String, Object>();
                    if (fileDataset.getColumns() != null) {
                        for (FileColumn column : fileDataset.getColumns()) {
                            if ("integer".equals(column.getValueTypeName())) {
                                fields.put(column.getName(), fieldSet.readInt(column.getColumnIndex() - 1));
                            } else {
                                fields.put(column.getName(), fieldSet.readString(column.getColumnIndex() - 1));
                            }
                        }
                    }
                    return fields;
                }
            });
        }});
    }
}