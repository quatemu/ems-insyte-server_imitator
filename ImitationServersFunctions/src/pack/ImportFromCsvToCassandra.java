package pack;


import com.insyte.ems.utils.parser.csv.CsvParser;
import com.insyte.ems.utils.parser.csv.Measure;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;

public class ImportFromCsvToCassandra {
    public void Import(FileReader fileReader, String dateTimeStringFormat) {
        CsvParser parser = new CsvParser(new BufferedReader(fileReader), dateTimeStringFormat);
        ArrayList<Measure> list = parser.getMeasures();

        CassandraConnector client = new CassandraConnector();
        client.connect();
        System.out.println("connected");

        client.createKeyspace();
        System.out.println("keyspace created");

        client.createTable();
        System.out.println("table created");

        client.insertDatas(list);
        System.out.println("datas inserted");

        client.close();
        System.out.println("closed");


        //client.selectAll();
        //System.out.println("datas selected");
    }
}
