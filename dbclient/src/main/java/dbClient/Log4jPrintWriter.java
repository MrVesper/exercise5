package dbClient;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;

/**
 * Created by maro on 2014-06-03.
 */
class Log4jPrintWriter extends PrintWriter {
	private static final Logger log= LogManager.getLogger(Log4jPrintWriter.class);

    Log4jPrintWriter() {
        super(new Log4jWriter());
    }

    private static class Log4jWriter extends Writer {
        @Override
        public void write(char[] cbuf, int off, int len) throws IOException {
            log.info(new String(cbuf, off, len));
        }

        @Override
        public void flush() throws IOException {
            // empty
        }

        @Override
        public void close() throws IOException {
            // empty
        }
    }
}
