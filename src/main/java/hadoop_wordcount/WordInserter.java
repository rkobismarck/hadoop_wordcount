package hadoop_wordcount;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;


public class WordInserter {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }
    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration confHbase = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(confHbase);

        Table table = connection.getTable(TableName.valueOf("test"));
        try {
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

            table.put(put1);
            table.put(put2);
            table.put(put3);

        } finally {
            table.close();
            connection.close();
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word inserter");
        job.setJarByClass(WordInserter.class);
        job.setMapperClass(WordInserter.TokenizerMapper.class);
        job.setCombinerClass(WordInserter.IntSumReducer.class);
        job.setReducerClass(WordInserter.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
