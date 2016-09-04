package cxp.ingest;

import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.batch.item.file.transform.IncorrectTokenCountException;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Created by markmo on 3/05/15.
 */
public class MetadataDrivenDelimitedLineTokenizer extends DelimitedLineTokenizer {

    private static final char START_XML_CHAR = '<';

    // space, tab, carriage return, newline, formfeed
    private static final List<Character> whitespaceCharacters = Arrays.asList(' ', '\t', '\r', '\n', '\f');

    // the delimiter character used when reading input.
    private String delimiter;

    private char quoteCharacter = DEFAULT_QUOTE_CHARACTER;

    private String quoteString;

    private Collection<Integer> includedFields = null;

    private FileDataset fileDataset;

    public MetadataDrivenDelimitedLineTokenizer(FileDataset fileDataset) {
        super(fileDataset.getColumnDelimiter());
        this.fileDataset = fileDataset;
        String delimiter = fileDataset.getColumnDelimiter();
        this.delimiter = delimiter;
        setDelimiter(delimiter);
    }

    /**
     * Public setter for the quoteCharacter. The quote character can be used to
     * extend a field across line endings or to enclose a String which contains
     * the delimiter. Inside a quoted token the quote character can be used to
     * escape itself, thus "a""b""c" is tokenized to a"b"c.
     *
     * @param quoteCharacter the quoteCharacter to set
     * @see #DEFAULT_QUOTE_CHARACTER
     */
    public void setQuoteCharacter(char quoteCharacter) {
        super.setQuoteCharacter(quoteCharacter);
        this.quoteCharacter = quoteCharacter;
        this.quoteString = "" + quoteCharacter;
    }

    @Override
    public FieldSet tokenize(String line) {
        if (line == null || line.trim().isEmpty()) return null;
        try {
            return super.tokenize(line);
        } catch (IncorrectTokenCountException e) {
            // TODO
            // depends on the footer row having a different column count
            if (fileDataset.isFooterRow()) {
                return null;
            } else {
                throw e;
            }
        }
    }

    /**
     * Yields the tokens resulting from the splitting of the supplied
     * <code>line</code>.
     *
     * @param line the line to be tokenized
     * @return the resulting tokens
     */
    @Override
    protected List<String> doTokenize(String line) {
        List<String> tokens = new ArrayList<String>();

        // line is never null in current implementation
        // line is checked in parent: AbstractLineTokenizer.tokenize()
        char[] chars = line.toCharArray();
        boolean inQuoted = false;
        boolean inTokenContent = false;
        boolean inXML = false;
        boolean outsideXMLTag = true;
        int lastCut = 0;
        int length = chars.length;
        int fieldCount = 0;
        int endIndexLastDelimiter = -1;

        for (int i = 0; i < length; i++) {
            char currentChar = chars[i];
            boolean isEnd = (i == (length - 1));
            boolean isDelimiter = isDelimiter(chars, i, delimiter, endIndexLastDelimiter);
            boolean isQuoteChar = isQuoteCharacter(currentChar);

            if (!inTokenContent && !isQuoteChar) {
                if (!isWhitespaceCharacter(currentChar)) {
                    if (START_XML_CHAR == currentChar) {
                        inXML = true;
                    }
                    inTokenContent = true;
                }
            }
            if (inXML) {
                outsideXMLTag = isCompleteXMLFragment(new String(chars, lastCut, length - lastCut));
            }
            if ((isDelimiter && !inQuoted) || isEnd) {
                endIndexLastDelimiter = i;
                int endPosition = (isEnd ? (length - lastCut) : (i - lastCut));

                if (isEnd && isDelimiter) {
                    endPosition = endPosition - delimiter.length();
                } else if (!isEnd) {
                    endPosition = (endPosition - delimiter.length()) + 1;
                }

                if (includedFields == null || includedFields.contains(fieldCount)) {
                    String value = maybeStripQuotes(new String(chars, lastCut, endPosition));
                    tokens.add(value);
                }

                fieldCount++;

                if (isEnd && (isDelimiter)) {
                    if (includedFields == null || includedFields.contains(fieldCount)) {
                        tokens.add("");
                    }
                    fieldCount++;
                }

                lastCut = i + 1;
                inTokenContent = false;
                inXML = false;
                outsideXMLTag = true;
            } else if (isQuoteChar && outsideXMLTag) {
                inQuoted = !inQuoted;
            }
        }

        return tokens;
    }

    /**
     * Determine if chars forms a complete XML fragment.
     *
     * @param chars
     * @return boolean true if XML is complete
     */
    private boolean isCompleteXMLFragment(String chars) {
        return StringUtils.countOccurrencesOf(chars, "<") == StringUtils.countOccurrencesOf(chars, ">");
    }

    /**
     * Determine if ch is a whitespace character.
     *
     * @param ch
     * @return boolean true if ch is a whitespace character
     */
    private boolean isWhitespaceCharacter(char ch) {
        // test for space, tab, carriage return, newline, formfeed
        return whitespaceCharacters.contains(ch);
    }

    /**
     * If the string is quoted strip (possibly with whitespace outside the
     * quotes (which will be stripped), replace escaped quotes inside the
     * string. Quotes are escaped with double instances of the quote character.
     *
     * @param string
     * @return the same string but stripped and unescaped if necessary
     */
    private String maybeStripQuotes(String string) {
        String value = string.trim();
        if (isQuoted(value)) {
            value = StringUtils.replace(value, "" + quoteCharacter + quoteCharacter, "" + quoteCharacter);
            int endLength = value.length() - 1;
            // used to deal with empty quoted values
            if (endLength == 0) {
                endLength = 1;
            }
            value = value.substring(1, endLength);
            return value;
        }
        return string;
    }

    /**
     * Is this string surrounded by quote characters?
     *
     * @param value
     * @return true if the value starts and ends with the
     * {@link #quoteCharacter}
     */
    private boolean isQuoted(String value) {
        return (value.startsWith(quoteString) && value.endsWith(quoteString));
    }

    /**
     * Is the supplied character the delimiter character?
     *
     * @param chars the character array to be checked
     * @return <code>true</code> if the supplied character is the delimiter
     * character
     * @see DelimitedLineTokenizer#DelimitedLineTokenizer(String)
     */
    private boolean isDelimiter(char[] chars, int i, String token, int endIndexLastDelimiter) {
        boolean result = false;

        if (i - endIndexLastDelimiter >= delimiter.length()) {
            if (i >= token.length() - 1) {
                String end = new String(chars, (i - token.length()) + 1, token.length());
                if (token.equals(end)) {
                    result = true;
                }
            }
        }

        return result;
    }
}
