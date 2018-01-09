package logProducer;

import org.apache.kafka.common.serialization.Serializer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class LogClass {

    private String logId;
    private int logCount;
    //probably this is the problem
    public List<String> logs;

    public LogClass() {
        logCount = 0;
        logs = new ArrayList<>();
    }

    public LogClass(String path) {
        this();
        fillInLogs(path);
    }

    public LogClass(File file) {
        this(file.getPath());
    }

    public String getLogId() {
        return logId;
    }

    public void setLogId(File file) {
        logId = file.getPath();
    }

    public void setLogId(String id) {
        logId = id;
    }

    public int getLogCount() {
        return logCount;
    }

    public void setLogCount(int count) {
        logCount = count;
    }

    public void fillInLogs(String path) {
        try {

            //FILL IN LOGS WITH EACH LOG SEGMENTATIONS
            FileInputStream logFile;
            logFile = new FileInputStream(path);
            BufferedReader br = new BufferedReader(new InputStreamReader(logFile));
            String prev = "";
            String strLine = "";
            while((strLine = br.readLine()) != null) {
                if (validateBeginning(strLine)) {
                    if (!prev.equals("")) {
                        logs.add(prev);
                    }
                    prev = strLine;
                } else {
                    prev = prev + strLine;
                }
            }
            if (!prev.equals("")) {
                logs.add(prev);
            }

            //SET FILE_PATH AS THE LOG ID;
            setLogId(path);

            //SET NUMBERS OF LOG FILES
            setLogCount(logs.size());

            logFile.close();

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    public void fillInLogs(File file) {
        fillInLogs(file.getPath());
    }

    private boolean validateBeginning(String str) {
        if (str.length() < 2) return false;
        return str.charAt(0) == 'u' && str.charAt(1) == '>';
    }
}
