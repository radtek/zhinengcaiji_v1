import javax.servlet.jsp.jstl.sql.Result;

import util.CommonDB;

public class IGPGBK
{
	public static void main(String[] args) throws Exception
	{
		Result r = CommonDB.queryForResult("select task_describe from igp_conf_task where task_id=110307");
		System.out.println(r.getRows()[0]);
	}
}
