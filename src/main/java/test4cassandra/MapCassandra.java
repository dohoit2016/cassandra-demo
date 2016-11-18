package test4cassandra;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapCassandra extends Mapper<LongWritable, Text, Void, GenericRecord> {
	
	public static String host = Cassandra.host;
	public static Schema schema;
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Void, GenericRecord>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		Parser parser = new Schema.Parser();
		System.out.println("Map setup");
		
		FileSystem fs = FileSystem.get(context.getConfiguration());
		Path path = new Path(host + "/user/donnn/parquet/schema.json");
		
		schema = parser.parse(fs.open(path));
//		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path), "utf8"));
//		String line = br.readLine();
//		while (line != null){
//			System.out.println(line);
//			line = br.readLine();
//		}
		
		
	}

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Void, GenericRecord>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		GenericRecord datum = new GenericData.Record(schema);
		String line = value.toString();
		String[] fields = line.split("\t");
		if (fields.length >= 19){
			try {
				System.out.println(fields);
				datum.put("timeCreate"   , fields[0]);
				datum.put("cookieCreate" , fields[1]);
//			System.out.println("time " + fields[0] + " ___ " + "cookie " + fields[1]);
				datum.put("browserCode"  , Integer.parseInt(fields[2]));
				datum.put("browserVer"   , fields[3]);
				datum.put("osCode"       , Integer.parseInt(fields[4]));
				datum.put("osVer"        , fields[5]);
				datum.put("ip"           , Long.parseLong(fields[6]));
				datum.put("locId"        , Integer.parseInt(fields[7]));
				datum.put("domain"       , fields[8]);
				datum.put("siteId"       , Integer.parseInt(fields[9]));
				datum.put("cId"          , Integer.parseInt(fields[10]));
				datum.put("path"         , fields[11]);
				datum.put("referer"      , fields[12]);
				datum.put("guid"         , Long.parseLong(fields[13]));
				datum.put("flashVersion" , fields[14]);
				datum.put("jre"          , fields[15]);
				datum.put("sr"           , fields[16]);
				datum.put("sc"           , fields[17]);
				datum.put("geographic"   , Integer.parseInt(fields[18]));
//			datum.put
				// context.write(new Text("A"), datum);
				context.write(null, datum);
				
			}
			catch (Exception e){
				System.out.println("Mark...");
				System.out.println(e.toString());
			}
			
		}
		
	}
	
}
