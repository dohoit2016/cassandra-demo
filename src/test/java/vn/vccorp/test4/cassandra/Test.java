package vn.vccorp.test4.cassandra;

import static java.lang.System.out;

import java.util.Date;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

public class Test {
	
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
		
		cluster = Cluster.builder().addContactPoint(node).withCredentials(usercame, password).withPort(portConn).build();
		cluster.init();
		System.out.println("He");
		final Metadata metadata = cluster.getMetadata();
		out.printf("Connected to cluster: %s\n", metadata.getClusterName());
		for (final Host host : metadata.getAllHosts()) {
			out.printf("Datacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(), host.getRack());
		}
		session = cluster.connect(keySpace);
		out.println("connected!");
		
//		ResultSet rs = session.execute("INSERT INTO test (id, detail) VALUES");
//		for(Row row : rs){
//			System.out.println(row.getString("guid"));
//		}
//		cluster.close();
		
//		PreparedStatement ps = session.prepare("INSERT INTO pageviewlog (guid, time_create) VALUES (?, ?)");
//		BoundStatement bs = ps.bind((long) 12346);
//		bs.setTimestamp("time_create", new Date());
//		BoundStatement bs = ps.bind(1, "male");
//		bs.setString("first_name", "hoa");
//		bs.setString("last_name", "nguyen tat");
//		bs.setString("gender", "male");
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
		System.out.println("done insert");
//		
		ResultSet rs = session.execute("SELECT * FROM pageviewlog");
		for(Row row : rs){
			System.out.println(row.getLong("guid"));
		}
		System.out.println("end");
		cluster.close();

	}

}
