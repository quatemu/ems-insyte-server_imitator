package pack;

import com.insyte.ems.utils.generation.GeneratorBuilder;
import com.insyte.ems.utils.generation.generators.GenerationResult;
import com.insyte.ems.utils.generation.generators.Generator;
import com.insyte.ems.utils.parser.csv.Measure;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

public class ImportFromGeneratorToCassandra {
    public void Import() {
        CassandraConnector client = new CassandraConnector();
        client.connect();
        System.out.println("connected");

        client.createKeyspace();
        System.out.println("keyspace created");

        client.createTable();
        System.out.println("table created");

        try(BufferedReader br = new BufferedReader(new FileReader("../generator-config.json"))) {
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();

            while (line != null) {
                sb.append(line);
                sb.append(System.lineSeparator());
                line = br.readLine();
            }
            GeneratorBuilder gb = new GeneratorBuilder();
            Generator generator = gb.getGenerator(sb.toString());

            List<GenerationResult> results = generator.getAll();
            List<Measure> measures = new ArrayList<>();
            for(GenerationResult result : results){
                Measure measure = new Measure();
                measure.DateTime = result.DateTime;
                measure.Value = result.Value;
                measures.add(measure);
            }

            client.insertDatas(measures);
            System.out.println("datas inserted");
        }
        catch (Exception ex){
            ex.printStackTrace();
        }

        client.close();
        System.out.println("closed");


        //client.selectAll();
        //System.out.println("datas selected");
    }
}
