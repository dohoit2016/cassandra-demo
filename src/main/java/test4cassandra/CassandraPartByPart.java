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

public class CassandraPartByPart {
	
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
//		Path path = new Path(host + "/user/donnn/cassandra/cassandra.in");
		Path path = new Path(host + "/data/rawText");
		RemoteIterator<LocatedFileStatus> ri = fs.listFiles(path, false);
		int stt = Integer.parseInt(args[0]);
		if (stt < 0){
			stt = 0;
		}
		BufferedWriter bw = null;
		Vector<LocatedFileStatus> vector = new Vector();
		while (ri.hasNext()){
			vector.add(ri.next());
		}
		int size = vector.size();
		while (stt < size){
			System.out.println("loop: " + stt);
			LocatedFileStatus lfs = vector.get(stt);
			try {
				System.out.println("processing " + stt + "/" + size + " : " + lfs.getPath().toString());
				bw = new BufferedWriter(new OutputStreamWriter(fs.append(new Path("/user/donnn/cassandra.progress.txt"))));
				bw.write("processing " + stt + "/" + size + " : " + lfs.getPath().toString() + "\n");
				bw.flush();
				Job job = new Job(conf, "PageViewLog");
				
				
				job.setJarByClass(CassandraPartByPart.class);
		
				FileInputFormat.addInputPath(job, lfs.getPath());
				FileOutputFormat.setOutputPath(job, new Path(host + "/user/donnn/cassandra/cassandra.chunk.out/" + stt));
				
				job.setMapperClass(MapCassandra.class);
				job.setNumReduceTasks(0);
		
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(Text.class);
				
				
				
				
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);
				
				
				
				job.waitForCompletion(true);
				bw.write("processed: " + lfs.getPath().toString() + "\n");
				bw.flush();
			}
			catch (Exception e){
				System.out.println(e.toString());
			}
			finally {
				stt++;
				try {
					bw.close();
				}
				catch (Exception e){
					System.out.println(e.toString());
				}
			}
		}
//		
//		schema = parser.parse(fs.open(path));
//		
//		
		
		System.out.println("Done import data to Cassandra");
//		
//		
		
		
		// End
		
		
		return ;
		
//		Schema schema = parser.parse(new FileInputStream("resource/schema.json"));

	}

}
