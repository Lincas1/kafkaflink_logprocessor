package logProducer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class LogFile {
    private String logId;//file name + log number
    //probably this is the problem
    private String log;

    public LogFile(String log) {
        this.log = log;
    }

    protected String getLog() {
        return this.log;
    }

    public String getLogId() {
        return logId;
    }

    public LogFile setLogId(String file, int num) {
        logId = file + Integer.toString(num);
        return this;
    }

    public LogFile setLogId(File file, int num) {
        return setLogId(file.getPath(), num);
    }
}
