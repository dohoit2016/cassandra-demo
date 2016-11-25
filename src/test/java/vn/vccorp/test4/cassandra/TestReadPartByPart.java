package vn.vccorp.test4.cassandra;

import static java.lang.System.out;

import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

public class TestReadPartByPart {
	
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
		
		

	}

}
