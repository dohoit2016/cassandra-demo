package vn.vccorp.test4.cassandra;

import static java.lang.System.out;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

import test4cassandra.Cassandra;

public class TestDirectImport {
	
	public static Cluster cluster;
	/** Cassandra Session. */
	public static Session session;
	
//	private static CassandraConnector instance;
	public static String usercame = "";
	public static String password = "";
	public static String node = "localhost";
	public static int portConn = 9042;
	public static String keySpace = "donnn";
	public static String table = "test";
	
	public static Date dateFromString(String s) throws ParseException{
//		String[] parts = s.split(" ");
//		String[] days = parts[0].split("-");
//		String[] times = parts[1].split(":");
//		int year = Integer.parseInt(days[0]);
//		int month = Integer.parseInt(days[1]);
//		int date = Integer.parseInt(days[2]);
//		
//		int hour = Integer.parseInt(times[0]);
//		int min = Integer.parseInt(times[1]);
//		int second = Integer.parseInt(times[2]);
////		Date d = new Date();
//		
//		
//		return new Date(year, month, date, hour, min, second);
		return (new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse(s));
	}

	public static void main(String[] args) throws IOException {
		
		Cluster cluster = Cluster.builder().addContactPoint("10.3.24.154").withCredentials("", "").build();
		cluster.init();
		System.out.println("OK");
		Session session = cluster.connect("donnn");
		PreparedStatement ps = session.prepare("INSERT INTO donnn.pageviewloga (time_create, cookie_create, browser_code, browser_ver, os_code, os_ver, ip, loc_id, domain, site_id, c_id, path, referer, guid, flash_version, jre, sr, sc, geographic, time_diff) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
		
//		PreparedStatement ps;
//		BoundStatement bs;
		
		
		
		Configuration conf = new Configuration();
		conf.addResource(new Path("resource/config/core-site.xml"));
		conf.addResource(new Path("resource/config/hbase-site.xml"));
		conf.addResource(new Path("resource/config/hdfs-site.xml"));
		conf.addResource(new Path("resource/config/mapred-site.xml"));
		conf.addResource(new Path("resource/config/yarn-site.xml"));
		
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(Cassandra.host + "/data/rawText/adv1475712754610.dat");
		
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
		String line = br.readLine();
		long count = 0;
		while (line != null){
			count++;
			
			if (count % 100 == 0){
				System.out.println("==========");
				System.out.println("count: " + count);
				System.out.println("==========");
			}
			
			
			
			String[] fields = line.split("\t");
			
			if (fields.length >=19 ){
				try {
					
					
					BoundStatement bs = ps.bind();
					
					
					bs.setDate("time_create"     , dateFromString(fields[0]));
					bs.setDate("cookie_create"   , dateFromString(fields[1]));
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
					long diff = dateFromString(fields[0]).getTime() - dateFromString(fields[1]).getTime();
					if (diff < 0){
						diff = 0;
					}
					diff = (long) diff / 1000 / 60;
					bs.setLong("time_diff", diff);
					
//					session.execute(bs);
					session.executeAsync(bs);
					
				}
				catch (Exception e){
					System.out.println(e.toString());
//					context.write(new Text("he: "), new Text(e.toString()));
				}
				
				// CREATE TABLE pageviewloga (time_create timestamp, cookie_create timestamp, browser_code int, browser_ver text, os_code int, os_ver text, ip bigint, loc_id int, domain text, site_id int, c_id int, path text, referer text, guid bigint, flash_version text, jre text, sr text, sc text, geographic int, time_diff bigint, PRIMARY KEY (guid, time_diff, time_create)) ;
				
			}
			else {
				System.out.println(fields.length + " : " + line);
			}
			line = br.readLine();
		}
		
		System.out.println("Done reading");
		
		
//		
		
		session.close();
		
		cluster.close();

	}

}
