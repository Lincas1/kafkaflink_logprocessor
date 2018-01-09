package KafkaProducer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class FileScan {

    @SuppressWarnings("unused")
    public FileScan() {
        ;
    }

    public static List<KafkaRecord> scanByFileName(String path) {
        List<KafkaRecord> logs = new ArrayList<>();
        try {
            //FILL IN LOGS WITH EACH LOG SEGMENTATIONS
            FileInputStream logFile;
            logFile = new FileInputStream(path);
            BufferedReader br = new BufferedReader(new InputStreamReader(logFile));
            String prev = "";
            String strLine = "";
            int count = 1;
            while((strLine = br.readLine()) != null) {
                if (validateBeginning(strLine)) {
                    if (!prev.equals("")) {
                        logs.add(new LogPiece(prev.substring(2)).setLogId(path, count++));
                    }
                    prev = strLine;
                } else {
                    prev = prev + strLine;
                }
            }
            if (!prev.equals("")) {
                logs.add(new LogPiece(prev.substring(2)).setLogId(path, count++));
            }

            logFile.close();

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }

        return logs;
    }

    public static List<KafkaRecord> scanByFileName(File file) {
        return scanByFileName(file.getPath());
    }

    private static boolean validateBeginning(String str) {
        if (str.length() < 2) return false;
        return str.charAt(0) == 'u' && str.charAt(1) == '>';
    }
}