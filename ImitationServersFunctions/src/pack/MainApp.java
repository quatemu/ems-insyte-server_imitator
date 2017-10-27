package pack;

import java.io.FileReader;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class MainApp {
    public static void main(String[] args){
        /*LocalDateTime ldt = LocalDateTime.parse("01.03.2017 12:27", DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm"));
        System.out.println(ldt);
        Timestamp ts = Timestamp.valueOf(ldt);
        System.out.println(ts);*/


        testImportFromCsvToCassandra();
    }

    private static void testImportFromCsvToCassandra() {
        try(FileReader fileReader = new FileReader("examples\\test_3.csv")){
            new ImportFromCsvToCassandra().Import(fileReader, "dd.MM.yyyy HH:mm");
        }
        catch (Exception ex){
            ex.printStackTrace();
        }
    }
}
