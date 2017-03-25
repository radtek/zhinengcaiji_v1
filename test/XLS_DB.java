import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
 


public class XLS_DB
{
	public static void main(String[] args) throws Exception
	{
		Class.forName("sun.jdbc.odbc.JdbcOdbcDriver");
		Connection con = DriverManager.getConnection("jdbc:odbc:bsc6000");
		Statement st = con.createStatement();
		ResultSet rs = st.executeQuery("select * from [Grid Results$]");
		while (rs.next()) {
			System.out.println(rs.getString(1)+"  "+rs.getString(2));
		}
	}
}
