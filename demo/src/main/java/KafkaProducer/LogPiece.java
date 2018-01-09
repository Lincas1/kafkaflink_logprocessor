package KafkaProducer;

import java.io.File;

public class LogPiece implements KafkaRecord<String, String> {
    private String logId;//file name + log number
    //probably this is the problem
    private String log;

    public LogPiece(String log) {
        this.log = log;
    }

    protected String getLog() {
        return this.log;
    }

    public String getLogId() {
        return logId;
    }

    public LogPiece setLogId(String file, int num) {
        logId = file + Integer.toString(num);
        log = logId + ">>" + log;
        return this;
    }

    public LogPiece setLogId(File file, int num) {
        return setLogId(file.getPath(), num);
    }

    @Override
    public String getKey() {
        return this.getLogId();
    }

    @Override
    public String getValue() {
        return this.getLog();
    }

    @Override
    public String getKeyAsString() {
        return this.getLogId();
    }

    @Override
    public String getValueAsString() {
        return this.getLog();
    }

}
