import java.lang.reflect.Field;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;

import oracle.jdbc.dbaccess.DBAccess;
import oracle.net.ns.NSProtocol;
import oracle.net.nt.ConnStrategy;
import oracle.net.resolver.AddrResolution;

public class FindOracleConnectionSocket
{
	public static void main(String[] args) throws Exception
	{
		// String sql =
		// "insert into igp_conf_rtask (test_col) values ('员工101【系统管理员】于2012-06-20 17:28:12对工单进行中途意见:详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容详细内容')";
		// Connection con = DbPool.getConn();
		// con.close();
		// CommonDB.executeUpdate(sql);

		Class.forName("oracle.jdbc.driver.OracleDriver");
		Connection con = DriverManager.getConnection("jdbc:oracle:thin:@192.168.15.12:1521:uway", "igp", "igp");
		Field f = con.getClass().getDeclaredField("db_access");
		System.out.println(f);
		DBAccess dba = (DBAccess) f.get(con);
		System.out.println(dba);
		for (Field fx : dba.getClass().getDeclaredFields())
			System.out.println(fx);
		f = dba.getClass().getDeclaredField("net");
		f.setAccessible(true);
		NSProtocol ns = (NSProtocol) f.get(dba);
		System.out.println(ns);
		f = ns.getClass().getDeclaredField("addrRes");
		f.setAccessible(true);
		AddrResolution addrRes = (AddrResolution) f.get(ns);
		f = addrRes.getClass().getDeclaredField("cs");
		f.setAccessible(true);
		ConnStrategy cs = (ConnStrategy) f.get(addrRes);
		f = cs.getOption().nt.getClass().getDeclaredField("socket");
		f.setAccessible(true);
		Socket s = (Socket) f.get(cs.getOption().nt);
		System.out.println(s);
		s.close();
		con.close();
	}
}
