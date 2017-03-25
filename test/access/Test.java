package access;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class Test {
	public static void main(String[] args) throws Exception {
		String driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
		String url = "jdbc:sqlserver://136.158.224.10:1433;DatabaseName=Bam";

		Class.forName(driver);

		Connection con = DriverManager.getConnection(url, "sa", "11111111");

		Statement st = con.createStatement();
		ResultSet rs = st
				.executeQuery("select   * from tbl_BtsBasicInfo  where iBtsId=3426");
		if (rs.next()) {
			System.out.println(rs.getInt(1));
			System.out.println(rs.getString(2));
		}
		rs.close();
		st.close();
		con.close();
	}
}
