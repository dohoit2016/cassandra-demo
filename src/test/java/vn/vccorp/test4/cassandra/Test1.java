package vn.vccorp.test4.cassandra;

import static java.lang.System.out;

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

public class Test1 {
	
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

	public static void main(String[] args) throws com.datastax.driver.core.exceptions.ReadTimeoutException, ParseException {
		
		Cluster cluster = Cluster.builder().addContactPoint("10.3.24.154").withCredentials("", "").build();
		cluster.init();
		System.out.println("OK");
		Session session = cluster.connect("donnn");
		
		PreparedStatement ps;
		BoundStatement bs;
		
		
		
		
		
		
		
		
//		ps = session.prepare("SELECT guid FROM donnn.pageviewlog where time_diff <= 30 ALLOW FILTERING");
		ps = session.prepare("SELECT domain, path, time_create FROM donnn.pageviewlog where guid = ?");
		
		bs = ps.bind(Long.parseLong(args[0]));
		
		System.out.println("bound");
		int pageSize = 100;
		bs.setFetchSize(pageSize);
		System.out.println("start executing");
//		bs = ps.bind(Long.parseLong("2937907151952574147"));
		
		
		
		Date day = (new SimpleDateFormat("yyyy-MM-dd")).parse(args[1]);
		
		ResultSet rs = session.execute(bs);
		
		for(Row row : rs){
			Date d = row.getDate("time_create");
			long diff = d.getTime() - day.getTime();
			if ((diff >= 0) && (diff < 86400000)){
				System.out.println(row.getString("domain") + row.getString("path"));
			}
		}
		
		session.close();
		cluster.close();
		System.out.println("done job");
		

	}

}
