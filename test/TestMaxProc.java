public class TestMaxProc
{
	public static void main(String[] args) throws Exception
	{

		final Process proc = Runtime.getRuntime().exec("sqlplus");
		new Thread(new Runnable()
		{

			@Override
			public void run()
			{
				try
				{
					Thread.sleep(2000);
				}
				catch (InterruptedException e)
				{
					e.printStackTrace();
				}
				proc.destroy();

			}
		}).start();
		System.err.println(proc.waitFor());

		// for (int i = 0; i < 1000; i++)
		// {
		// new TestThread().start();
		// }
	}

	static class TestThread extends Thread
	{
		@Override
		public void run()
		{
			try
			{
				Process p = Runtime.getRuntime().exec("sqlplus");
				p.waitFor();
				System.out.println(p.exitValue());
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}
	}
}
