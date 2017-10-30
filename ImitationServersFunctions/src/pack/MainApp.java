package pack;

import java.io.FileReader;

public class MainApp {
    public static void main(String[] args){
        /*LocalDateTime ldt = LocalDateTime.parse("01.03.2017 12:27", DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm"));
        System.out.println(ldt);
        Timestamp ts = Timestamp.valueOf(ldt);
        System.out.println(ts);*/


        testImportFromCsvToCassandra();
        //testImportFromGeneratorToCassandra();
    }

    private static void testImportFromCsvToCassandra() {
        try(FileReader fileReader = new FileReader("examples\\Умный дом.csv")){
            new ImportFromCsvToCassandra().Import(fileReader, "dd.MM.yyyy HH:mm");
        }
        catch (Exception ex){
            ex.printStackTrace();
        }
    }

    private static void testImportFromGeneratorToCassandra(){
        new ImportFromGeneratorToCassandra().Import();
    }
}
