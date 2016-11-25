package vn.vccorp.test4.cassandra;

import static java.lang.System.out;

import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
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
import com.datastax.driver.core.exceptions.ReadTimeoutException;

public class Test4 {
	
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

	public static void main(String[] args) throws com.datastax.driver.core.exceptions.ReadTimeoutException {
		
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
		
		
		
		
		
		
//		ps = session.prepare("SELECT guid FROM donnn.pageviewlog where time_diff <= 30 ALLOW FILTERING");
		ps = session.prepare("SELECT guid FROM donnn.pageviewlog");
		
//		bs = ps.bind(Long.parseLong(args[0]));
		bs = ps.bind();
		System.out.println("bound");
		int pageSize = 10;
		bs.setFetchSize(pageSize);
		System.out.println("start executing");
//		bs = ps.bind(Long.parseLong("2937907151952574147"));
		
		int check = 0;
		Set<Long> guids = new HashSet<Long>();
		
//		System.out.println("Processing..." + rs.all().size() + " row(s)...");
		int Case = 2;
		if (Case == 1){
			System.out.println("direct");
			ResultSet rs = session.execute(bs);
			System.out.println("done executation");
			
			int stt = 0;
			for(Row row : rs){
				if (stt % 1000 == 0){
					System.out.println(pageSize + " : " + stt + " : " + row.getLong("guid"));
				}
				stt++;
				guids.add(new Long(row.getLong("guid")));
			}
			
			System.out.println("Done processing.");
			
			for(Long guid : guids){
//				System.out.println(guid.longValue());
			}
			
			System.out.println("Prited " + guids.size() + " guid(s).");
			
			System.out.println("Done Job.");
			
			session.close();
			
			cluster.close();
		}
		
		else {
			System.out.println("try catch");
			try {
				ResultSet rs = session.execute(bs);
				System.out.println("done execute");
				
				int stt = 0;
				for(Row row : rs){
					if (stt % 1000 == 0){
						System.out.println(pageSize + " : " + stt + " : " + row.getLong("guid"));
					}
					stt++;
					guids.add(new Long(row.getLong("guid")));
				}
				
				System.out.println("Done processing.");
			}
			catch (Exception e){
				
			}
			
			finally{
				System.out.println("Prited " + guids.size() + " guid(s).");
				
				System.out.println("Done Job.");
				
				session.close();
				
				cluster.close();
			}
		}
		
		
		
		

	}

}
