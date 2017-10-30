package pack;

import com.datastax.driver.core.*;
import com.insyte.ems.utils.parser.csv.Measure;
import java.util.List;

public class CassandraConnector {
    private String KeyspaceName = "test_storage";
    private String MeasuresTableName = "measures";

    private Cluster cluster;
    private Session session;

    public void connect() {
        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect();
    }

    public Session getSession() {
        return this.session;
    }

    public void close() {
        session.close();
        cluster.close();
    }

    public void createKeyspace() {
        StringBuilder sb =
                new StringBuilder("CREATE KEYSPACE IF NOT EXISTS ")
                        .append(KeyspaceName)
                        .append(" WITH replication = {")
                        .append("'class':'SimpleStrategy'")
                        .append(",'replication_factor': 1")
                        .append("};");

        String query = sb.toString();
        session.execute(query);
    }


    public void createTable() {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(KeyspaceName + "." + MeasuresTableName).append("(")
                .append("date timestamp,")
                .append("device_id int,")
                .append("data_source_id int,")
                .append("value double,")
                .append("PRIMARY KEY(date, device_id, data_source_id));");

        String query = sb.toString();
        session.execute(query);
    }

    public void insertDatas(List<Measure> measures){
        int MAXBATCHSIZE = 20;

        long start = System.nanoTime();
        int currentBatchSize = 0;
        StringBuilder sb = new StringBuilder();
        for(Measure measure : measures){
            sb.append("INSERT INTO ")
                    .append(KeyspaceName + "." + MeasuresTableName)
                    .append("(date, device_id, data_source_id, value) ")
                .append("VALUES ('")
                    .append(measure.DateTime + "+0000',")
                    .append(measure.DeviceID + ",")
                    .append(measure.DataSourceID + ",")
                    .append(measure.Value)
                .append(");");
            if(++currentBatchSize >= MAXBATCHSIZE)
            {
                String query = "BEGIN BATCH "
                                + sb.toString() +
                                " APPLY BATCH;";
                session.execute(query);
                sb.setLength(0);
                currentBatchSize = 0;
            }
        }
        if(currentBatchSize != 0){
            String query = "BEGIN BATCH "
                            + sb.toString() +
                            " APPLY BATCH;";
            session.execute(query);
        }


        long end = System.nanoTime();
        System.out.print("Required time: ");
        System.out.println(end - start);
    }

    public void selectAll() {
        StringBuilder sb =
                new StringBuilder("SELECT * FROM " + KeyspaceName + "." + MeasuresTableName);

        String query = sb.toString();
        ResultSet rs = session.execute(query);

        rs.forEach(r -> {
            System.out.println(r.toString());
        });
    }
}
