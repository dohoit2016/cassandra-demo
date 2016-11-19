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

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

public class MapCassandra extends Mapper<LongWritable, Text, Void, Void> {
	
	public static String host = Cassandra.host;
	public static Schema schema;
	public static Cluster cluster;
	public static Session session;
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Void, Void>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
//		Parser parser = new Schema.Parser();
//		System.out.println("Map setup");
//		
//		FileSystem fs = FileSystem.get(context.getConfiguration());
//		Path path = new Path(host + "/user/donnn/parquet/schema.json");
		
//		schema = parser.parse(fs.open(path));
//		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path), "utf8"));
//		String line = br.readLine();
//		while (line != null){
//			System.out.println(line);
//			line = br.readLine();
//		}
		
//		cluster = Cluster.builder().addContactPoint("10.3.24.154").build();
		cluster = Cluster.builder().addContactPoint("10.3.24.154").withCredentials("", "").build();
		cluster.init();
		System.out.println("OK");
		session = cluster.connect("donnn");
		
		
	}

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Void, Void>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		
		
		String line = value.toString();
		String[] fields = line.split("\t");
		
		if (fields.length >=19 ){
			try {
				PreparedStatement ps = session.prepare("INSERT INTO pageviewlog (timeCreate, cookieCreate, browserCode, browserVer, osCode, osVer, ip, locId, domain, siteId, cId, path, referer, guid, flashVersion, jre, sr, sc, geographic) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
				
				BoundStatement bs = ps.bind();
				bs.setString("time_create"   , fields[0]);
				bs.setString("cookie_create" , fields[1]);
				bs.setInt("browser_code"     , Integer.parseInt(fields[2]));
				bs.setString("browser_ver"   , fields[3]);
				bs.setInt("os_code"          , Integer.parseInt(fields[4]));
				bs.setString("os_ver"        , fields[5]);
				bs.setLong("ip"              , Long.parseLong(fields[6]));
				bs.setInt("loc_id"           , Integer.parseInt(fields[7]));
				bs.setString("domain"        , fields[8]);
				bs.setInt("site_id"          , Integer.parseInt(fields[9]));
				bs.setInt("c_id"             , Integer.parseInt(fields[10]));
				bs.setString("path"          , fields[11]);
				bs.setString("referer"       , fields[12]);
				bs.setLong("guid"            , Long.parseLong(fields[13]));
				bs.setString("flash_version" , fields[14]);
				bs.setString("jre"           , fields[15]);
				bs.setString("sr"            , fields[16]);
				bs.setString("sc"            , fields[17]);
				bs.setInt("geographic"       , Integer.parseInt(fields[18]));
				
				session.execute(bs);
			}
			catch (Exception e){
				System.out.println(e.toString());
			}
			
			// CREATE TABLE pageviewlog (time_create timestamp, cookie_create timestamp, browser_code int, browser_ver text, os_code int, os_ver text, ip bigint, loc_id int, domain text, site_id int, c_id int, path text, referer text, guid bigint, flash_version text, jre text, sr text, sc text, geographic int, PRIMARY KEY (guid, time_create)) ;
			
		}
		else {
			System.out.println(fields.length + " : " + line);
		}
		
		
	}
	
}


