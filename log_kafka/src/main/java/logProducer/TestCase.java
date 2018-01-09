package logProducer;

import java.util.ArrayList;
import java.util.List;

public class TestCase {

    public static void main(String[] args) {
        String path = "displaypolicyd.log";
        List<LogFile> show = FileScan.scanByFileName(path);
        for (LogFile each : show) {
            System.out.println(each.getLog());
        }
        LogProducer logs = new LogProducer().setRecord(path).setTopic("test");
        System.out.println("123");
        logs.pushLog();
        System.out.println("123");
    }
}
