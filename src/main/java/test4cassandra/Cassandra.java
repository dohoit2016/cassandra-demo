package test4cassandra;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericRecord;
//import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Group;
import org.apache.hadoop.mapreduce.Job;

public class Cassandra {
	
	public static final String host = "hdfs://test1:9000";
	public static Schema schema;
	
	public static final int debug = 1;

	public static void main(String[] args) throws FileNotFoundException, IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration();
		conf.addResource(new Path("resource/config/core-site.xml"));
		conf.addResource(new Path("resource/config/hbase-site.xml"));
		conf.addResource(new Path("resource/config/hdfs-site.xml"));
		conf.addResource(new Path("resource/config/mapred-site.xml"));
		conf.addResource(new Path("resource/config/yarn-site.xml"));
		
//		Parser parser = new Schema.Parser();
		
//		Schema schema = parser.parse(Parquet.class.getResourceAsStream("schema.json"));
//		Schema schema = parser.parse((InputStream) new FileInputStream("/schema.json"));
		
//		
//		Parser parser = new Schema.Parser();
		System.out.println("Main");
		
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(host + "/data/rawText/");
		RemoteIterator<LocatedFileStatus> ri = fs.listFiles(path, false);
		while (ri.hasNext()){
			LocatedFileStatus lfs = ri.next();
			System.out.println(lfs.getPath().toString() + " : " + lfs.getSymlink().toString());
		}
//		
//		schema = parser.parse(fs.open(path));
//		
		Job job = new Job(conf, "PageViewLog");
		job.setJarByClass(Cassandra.class);

		FileInputFormat.addInputPath(job, new Path(host + "/data/rawText"));
		FileOutputFormat.setOutputPath(job, new Path(host + "/user/donnn/cassandra/cassandra.out"));
		
		job.setMapperClass(MapCassandra.class);
		job.setNumReduceTasks(0);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		
		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		
		
		job.waitForCompletion(true);
		
		System.out.println("Done import data to Cassandra");
//		
//		
		
		
		// End
		
		
		return ;
		
//		Schema schema = parser.parse(new FileInputStream("resource/schema.json"));

	}

}
