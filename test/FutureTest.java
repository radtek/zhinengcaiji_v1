import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import util.DbPool;

public class FutureTest
{
	public static void main(String[] args)
	{
		FutureTask<String> task = new FutureTask<String>(new Callable<String>()
		{
			@Override
			public String call() throws Exception
			{
				Connection con = DbPool.getConn();
				Statement st = con.createStatement();
				ResultSet rs = st.executeQuery("select * from all_objects");
				while (rs.next())
				{
					String owner = rs.getString(1);
					String objectname = rs.getString(2);
					owner = null;
					objectname = null;
				}
				rs.close();
				st.close();
				con.close();
				System.out.println("complete");
				return "ok";
			}
		});

		ExecutorService service = Executors.newFixedThreadPool(3);
		service.execute(task);
		try
		{
			System.out.println("result - " + task.get(1, TimeUnit.SECONDS));
		}
		catch (InterruptedException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (ExecutionException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (TimeoutException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("cancel - " + task.cancel(true));
	}
}
