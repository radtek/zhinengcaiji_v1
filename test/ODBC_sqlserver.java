import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class ODBC_sqlserver
{
	public static void main(String[] args) throws Exception
	{
		Class.forName("sun.jdbc.odbc.JdbcOdbcDriver");
		Connection con = DriverManager.getConnection("jdbc:odbc:test");

		Statement st = con.createStatement();
		ResultSet rs = st.executeQuery("select * from my_tbl");
		if (rs.next())
		{
			System.out.println(rs.getInt(1) + "   " + rs.getString(2));
		}
		con.close();

		for (int i = 0; i < 1000; i++)
		{
			st.executeUpdate("insert into my_tbl (name) values ('myname_" + i
					+ "')");
			System.out.println("ok");
		}
		st.close();
		con.close();
	}
}
