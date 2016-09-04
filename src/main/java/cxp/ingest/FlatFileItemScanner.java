package cxp.ingest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.ReaderNotOpenException;
import org.springframework.batch.item.file.*;
import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy;
import org.springframework.batch.item.file.separator.RecordSeparatorPolicy;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Scanner;

/**
 * Created by markmo on 30/06/15.
 */
public class FlatFileItemScanner<T> extends FlatFileItemReader<T> {

    private static final Log log = LogFactory.getLog(FlatFileItemScanner.class);

    private Resource resource;

    private Scanner scanner;

    private RecordSeparatorPolicy recordSeparatorPolicy = new DefaultRecordSeparatorPolicy();

    private LineMapper<T> lineMapper;

    private int lineCount = 0;

    private String[] comments = new String[] { "#" };

    private boolean noInput = false;

    private int linesToSkip = 0;

    private LineCallbackHandler skippedLinesCallback;

    private boolean strict = true;

    private String rowDelimiter;

    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.notNull(lineMapper, "LineMapper is required");
    }

    /**
     * In strict mode the reader will throw an exception on
     * {@link #open(org.springframework.batch.item.ExecutionContext)} if the input resource does not exist.
     * @param strict <code>true</code> by default
     */
    public void setStrict(boolean strict) {
        this.strict = strict;
    }

    /**
     * @param skippedLinesCallback will be called for each one of the initial skipped lines before any items are read.
     */
    public void setSkippedLinesCallback(LineCallbackHandler skippedLinesCallback) {
        this.skippedLinesCallback = skippedLinesCallback;
    }

    /**
     * Public setter for the number of lines to skip at the start of a file. Can be used if the file contains a header
     * without useful (column name) information, and without a comment delimiter at the beginning of the lines.
     *
     * @param linesToSkip the number of lines to skip
     */
    public void setLinesToSkip(int linesToSkip) {
        this.linesToSkip = linesToSkip;
    }

    private boolean isComment(String line) {
        for (String prefix : comments) {
            if (line.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Public setter for the input resource.
     */
    @Override
    public void setResource(Resource resource) {
        this.resource = resource;
    }

    /**
     * Setter for line mapper. This property is required to be set.
     * @param lineMapper maps line to item
     */
    public void setLineMapper(LineMapper<T> lineMapper) {
        this.lineMapper = lineMapper;
    }

    /**
     * Public setter for the recordSeparatorPolicy. Used to determine where the line endings are and do things like
     * continue over a line ending if inside a quoted string.
     *
     * @param recordSeparatorPolicy the recordSeparatorPolicy to set
     */
    public void setRecordSeparatorPolicy(RecordSeparatorPolicy recordSeparatorPolicy) {
        this.recordSeparatorPolicy = recordSeparatorPolicy;
    }

    /**
     * @return string corresponding to logical record according to
     * {@link #setRecordSeparatorPolicy(RecordSeparatorPolicy)} (might span multiple lines in file).
     */
    @Override
    protected T doRead() throws Exception {
        if (noInput) {
            return null;
        }

        String line = readLine();

        if (line == null) {
            return null;
        }
        else {
            try {
                return lineMapper.mapLine(line, lineCount);
            }
            catch (Exception ex) {
                throw new FlatFileParseException("Parsing error at line: " + lineCount + " in resource=["
                        + resource.getDescription() + "], input=[" + line + "]", ex, line, lineCount);
            }
        }
    }

    /**
     * @return next line (skip comments).getCurrentResource
     */
    private String readLine() {

        if (scanner == null) {
            throw new ReaderNotOpenException("Scanner must be open before it can be read.");
        }

        String line = null;

        try {
            try {
                line = this.scanner.next();
            } catch (NoSuchElementException e) {
                return null;
            }
            if (line == null) {
                return null;
            }
            lineCount++;
            while (isComment(line)) {
                line = scanner.next();
                if (line == null) {
                    return null;
                }
                lineCount++;
            }

            line = applyRecordSeparatorPolicy(line);
        }
        catch (IOException e) {
            // Prevent IOException from recurring indefinitely
            // if client keeps catching and re-calling
            noInput = true;
            throw new NonTransientFlatFileException("Unable to read from resource: [" + resource + "]", e, line,
                    lineCount);
        }
        return line;
    }

    @Override
    protected void doClose() throws Exception {
        lineCount = 0;
        if (scanner != null) {
            scanner.close();
        }
    }

    @Override
    protected void doOpen() throws Exception {
        Assert.notNull(resource, "Input resource must be set");
        Assert.notNull(recordSeparatorPolicy, "RecordSeparatorPolicy must be set");

        noInput = true;
        if (!resource.exists()) {
            if (strict) {
                throw new IllegalStateException("Input resource must exist (reader is in 'strict' mode): " + resource);
            }
            log.warn("Input resource does not exist " + resource.getDescription());
            return;
        }

        if (!resource.isReadable()) {
            if (strict) {
                throw new IllegalStateException("Input resource must be readable (reader is in 'strict' mode): "
                        + resource);
            }
            log.warn("Input resource is not readable " + resource.getDescription());
            return;
        }

        scanner = new Scanner(resource.getInputStream(), DEFAULT_CHARSET);
        scanner.useDelimiter(rowDelimiter);
        for (int i = 0; i < linesToSkip; i++) {
            String line = readLine();
            if (skippedLinesCallback != null) {
                skippedLinesCallback.handleLine(line);
            }
        }
        noInput = false;
    }

    public void setRowDelimiter(String lineTerminator) {
        this.rowDelimiter = lineTerminator;
    }

    private String applyRecordSeparatorPolicy(String line) throws IOException {
        String record = line;
        while (line != null && !recordSeparatorPolicy.isEndOfRecord(record)) {
            line = this.scanner.nextLine();
            if (line == null) {
                if (StringUtils.hasText(record)) {
                    // A record was partially complete since it hasn't ended but
                    // the line is null
                    throw new FlatFileParseException("Unexpected end of file before record complete", record, lineCount);
                }
                else {
                    // Record has no text but it might still be post processed
                    // to something (skipping preProcess since that was already
                    // done)
                    break;
                }
            }
            else {
                lineCount++;
            }
            record = recordSeparatorPolicy.preProcess(record) + line;
        }
        return recordSeparatorPolicy.postProcess(record);
    }
}
