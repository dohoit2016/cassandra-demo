package vn.vccorp.test4.cassandra;

import static java.lang.System.out;

import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.Map.Entry;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

public class Test3 {
	
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

	public static void main(String[] args) {
		
		Cluster cluster = Cluster.builder().addContactPoint("10.3.24.154").withCredentials("", "").build();
		cluster.init();
		System.out.println("OK");
		Session session = cluster.connect("donnn");
		
		PreparedStatement ps;
		BoundStatement bs;
		
		// TODO Auto-generated method stub
		
//		Cluster cluster = Cluster.builder().addContactPoint("localhost").build();
//		cluster.init();
//		System.out.println("OK");
//		Session session = cluster.connect("my_keyspace");
		
		
//		PreparedStatement ps = session.prepare("INSERT INTO user (first_name, gender, last_name) VALUES (?, ?, ?)");
//		BoundStatement bs = ps.bind("hehe", "male", "hihi");
//		bs.setString("first_name", "hoa");
//		bs.setString("last_name", "nguyen tat");
//		bs.setString("gender", "male");
//		session.execute(bs);
		
//		System.out.println("done insert");
		
//		ResultSet rs = session.execute("SELECT * FROM user");
//		for(Row row : rs){
//			System.out.println(row.getString("first_name"));
//		}
//		cluster.close();
		
//		cluster = Cluster.builder().addContactPoint(node).withCredentials(usercame, password).withPort(portConn).build();
//		cluster.init();
//		System.out.println("He");
//		final Metadata metadata = cluster.getMetadata();
//		out.printf("Connected to cluster: %s\n", metadata.getClusterName());
//		for (final Host host : metadata.getAllHosts()) {
//			out.printf("Datacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(), host.getRack());
//		}
//		session = cluster.connect(keySpace);
//		out.println("connected!");
		
//		Date
		
//		ResultSet rs = session.execute("INSERT INTO test (id, detail) VALUES");
//		for(Row row : rs){
//			System.out.println(row.getString("guid"));
//		}
//		cluster.close();
		
//		ps = session.prepare("INSERT INTO donnn.pageviewlog (guid, time_create) VALUES (?, ?)");
//		bs = ps.bind(Long.parseLong("2937907151952574147"));
//		bs.setDate("time_create", new Date(2016, 10, 7, 1, 25, 40));
//		session.execute(bs);
		
//		PreparedStatement ps = session.prepare("INSERT INTO test (id, detail) VALUES (?, ?)");
//		BoundStatement bs = ps.bind(12346, "server test detail");
//		bs.setTimestamp("time_create", new Date());
//		BoundStatement bs = ps.bind(1, "male");
//		bs.setString("first_name", "hoa");
//		bs.setString("last_name", "nguyen tat");
//		bs.setString("gender", "male");
//		session.execute(bs);
//		PreparedStatement ps = session.prepare("S)
//		
//		System.out.println("done insert");
////		
//		ResultSet rs = session.execute("SELECT * FROM pageviewlog");
//		for(Row row : rs){
//			System.out.println(row.getLong("guid"));
//		}
//		System.out.println("end");
//		cluster.close();
		
		
		
		// Job 2
		
		
		ps = session.prepare("SELECT time_create FROM donnn.pageviewlog WHERE guid = ?");
		
		bs = ps.bind(Long.parseLong(args[0]));
//		bs = ps.bind(Long.parseLong("2937907151952574147"));
		
		int check = 0;
		ResultSet rs = session.execute(bs);
		Date recent = new Date();
		
		for(Row row : rs){
			if (check == 0){
				check = 1;
				recent = row.getDate("time_create");
			}
			else {
				Date curDate = row.getDate("time_create");
				if (curDate.getTime() - recent.getTime() > 0){
					recent = curDate;
				}
			}
		}
		
		if (check == 1){
			System.out.println(recent);
		}
		else {
			System.out.println("GUID not found.");
		}
		
		System.out.println("Done Job.");
		
		session.close();
		
		cluster.close();

	}

}
