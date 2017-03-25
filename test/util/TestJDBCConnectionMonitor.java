package util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class TestJDBCConnectionMonitor
{

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception
	{
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		// Connection con =
		// DriverManager.getConnection("jdbc:sqlserver://136.160.65.35:1433;DatabaseName=Bam",
		// "sa", "11111111");
		//

		Class.forName("com.sybase.jdbc3.jdbc.SybDriver");
		Connection con = DriverManager.getConnection("jdbc:sybase:Tds:136.160.7.165:4100/pmdb", "sa", "emsems");
		JDBCConnectionMonitor monitor = new JDBCConnectionMonitor(123, con, 1);
		monitor.startMonit();
		PreparedStatement ps = con.prepareStatement("select * from odtMoc_CDMADOCH_DOBCAP");
		ResultSet rs = ps.executeQuery();
		while (rs.next())
		{
			System.out.println(rs.getString(1));
		}
		rs.close();
		ps.close();
		con.close();
		monitor.endMonit();
	}

}
