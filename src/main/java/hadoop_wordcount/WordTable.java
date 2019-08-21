package hadoop_wordcount;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class WordTable {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        try {
            BufferedMutator table = connection.getBufferedMutator(TableName.valueOf("test"));
            Put put1 = new Put(Bytes.toBytes("row1"));
            Put put2 = new Put(Bytes.toBytes("row2"));
            Put put3 = new Put(Bytes.toBytes("row3"));
            put1.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("qual1"),
                    Bytes.toBytes("ValueOneForPut1Qual1"));
            put2.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("qual1"),
                    Bytes.toBytes("ValueOneForPut2Qual1"));
            put3.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("qual1"),
                    Bytes.toBytes("ValueOneForPut2Qual1"));
            put1.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("qual2"),
                    Bytes.toBytes("ValueOneForPut1Qual2"));
            put2.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("qual2"),
                    Bytes.toBytes("ValueOneForPut2Qual2"));
            put3.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("qual2"),
                    Bytes.toBytes("ValueOneForPut3Qual3"));
            table.mutate(put1);
            table.mutate(put2);
            table.mutate(put3);
            table.flush();
        } finally {
            connection.close();
        }
    }
}
