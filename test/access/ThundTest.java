package access;

import java.util.ArrayList;
import java.util.List;

public class ThundTest
{

	/**
	 * @param args
	 */
	public static void main(String[] args)
	{
		List<Thread> list = new ArrayList<Thread>();

		for (int i = 0; i < 10; i++)
		{
			initThreads(list);
			for (Thread th : list)
			{
				th.start();
				try
				{
					th.join();
				}
				catch (InterruptedException e)
				{
				}
			}
			list.clear();
		}
	}

	private static void initThreads(List<Thread> list)
	{
		list.add(new MyThread("a"));
		list.add(new MyThread("b"));
		list.add(new MyThread("c"));
	}

}

class MyThread extends Thread
{

	public MyThread(String name)
	{
		super(name);
	}

	@Override
	public void run()
	{
		System.out.print(getName());
	}
}
