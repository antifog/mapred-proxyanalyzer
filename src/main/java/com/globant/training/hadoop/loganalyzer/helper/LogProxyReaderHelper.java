package com.globant.training.hadoop.loganalyzer.helper;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Helper class design to parse the given logs.
 *
 * @author armando.delgado
 */

public class LogProxyReaderHelper {

    /**
     * RegEx Pattern used to parse
     */
    private final static String logPattern = ".*size=\"(.*?)\".*url=\"(.*?)\" .*fullreqtime=\"(.*?)\".*";

    private static LogProxyReaderHelper instance = null;

    /**
     * Private constructor regarding to the singleton pattern
     */
    private LogProxyReaderHelper() {

    }

    /**
     * @return The only class instance
     */
    public static LogProxyReaderHelper getInstance() {
        if (instance == null)
            instance = new LogProxyReaderHelper();

        return instance;
    }

    /**
     * @param line whit the information to be parsed
     * @return String array with the parsed values
     */
    public String[] parseProxyLogInfo(String line) {
        Pattern extractor = Pattern.compile(logPattern);
        Matcher matcher = extractor.matcher(line);

        String[] response = null;
        if (matcher.find()) {
            response = new String[3];
            response[0] = matcher.group(1);
            response[1] = matcher.group(2);
            response[2] = matcher.group(3);
        } else
            return null;

        return response;
    }
}
