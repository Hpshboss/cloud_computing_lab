import java.io.IOException;
import java.util.*;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class hbaseMapredOutput {
    public static class hbaseMapredOutputMapper extends TableMapper<Text, MapWritable> {
        public void map(ImmutableBytesWritable key, Result value, Context context)
                throws IOException, InterruptedException {
            MapWritable outputMap = new MapWritable();
            for (Cell kv : value.rawCells()) {
                outputMap.put(new Text("family"), new Text(Arrays.copyOfRange(kv.getFamilyArray(), kv.getFamilyOffset(),
                        kv.getFamilyOffset() + kv.getFamilyLength())));
                outputMap.put(new Text("qualifier"), new Text(Arrays.copyOfRange(kv.getQualifierArray(),
                        kv.getQualifierOffset(), kv.getQualifierOffset() + kv.getQualifierLength())));
                outputMap.put(new Text("value"), new Text(Arrays.copyOfRange(kv.getValueArray(), kv.getValueOffset(),
                        kv.getValueOffset() + kv.getValueLength())));
                context.write(new Text(
                        Arrays.copyOfRange(kv.getRowArray(), kv.getRowOffset(), kv.getRowOffset() + kv.getRowLength())),
                        outputMap);
                outputMap.clear();
            }
        }
    }

    public static class hbaseMapredOutputReducer extends Reducer<Text, MapWritable, Text, Text> {
        public void reduce(Text key, Iterable<MapWritable> values, Context context)
                throws IOException, InterruptedException {
            for (MapWritable valueObject : values) {
                String family = valueObject.get(new Text("family")).toString();
                String qualifier = valueObject.get(new Text("qualifier")).toString();
                String value = valueObject.get(new Text("value")).toString();
                context.write(key, new Text(family + " " + qualifier + " " + value));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("please enter output, tablename");
            System.exit(0);
        }

        String output = args[0];
        String tablename = args[1];
        String username = "<studentID>"; // TODO change this

        Configuration config = HBaseConfiguration.create();
        config.set(TableInputFormat.INPUT_TABLE, tablename);

        // These two lines are only needed when you run on the server
        config.set("hbase.zookeeper.quorum", "hadoop-slave1,hadoop-slave2,hadoop-slave3,hadoop-master");
        config.set("zookeeper.znode.parent", "/hbase-unsecure");

        Job job = Job.getInstance(config);
        job.setJobName("hbaseMapredOutput_" + username);

        job.setJarByClass(hbaseMapredOutput.class);
        job.setMapperClass(hbaseMapredOutputMapper.class);
        job.setReducerClass(hbaseMapredOutputReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setInputFormatClass(TableInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);
    }
}