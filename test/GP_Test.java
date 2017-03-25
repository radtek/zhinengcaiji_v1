import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Enumeration;

import util.CommonDB;

public class GP_Test
{
	public static void main(String[] args)
	{
		Connection con = null;
		ResultSet rs = null;
		Statement st = null;
		try
		{
			Class.forName("org.postgresql.Driver");
			con = DriverManager.getConnection("jdbc:postgresql://192.168.0.215:5432/cnoap", "noap", "uwaysoft2009");
			st = con.createStatement();
			System.out.println(Runtime.getRuntime().freeMemory());
			rs = st.executeQuery("select * from r_ne_cell_c_bj");
			if ( rs.next() )
				System.out.println(rs.getString("china_name"));
			System.out.println(Runtime.getRuntime().freeMemory());
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			CommonDB.close(rs, st, con);
			Enumeration<Driver> em = DriverManager.getDrivers();
			while (em.hasMoreElements())
			{
				Driver dr = em.nextElement();
				if ( dr != null )
					try
					{
						DriverManager.deregisterDriver(dr);
					}
					catch (SQLException e)
					{
						e.printStackTrace();
					}
			}
		}
	}
}
