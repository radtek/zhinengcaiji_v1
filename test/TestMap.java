import java.util.HashMap;

class MyHashMap extends HashMap
{
	@Override
	protected void finalize() throws Throwable
	{
		System.out.println("finalize");
	}

}

public class TestMap
{
	public static void main(String[] args) throws Exception
	{
		test();
		for (int i = 0; i < 100; i++)
		{
			System.gc();
			Thread.sleep(10);
		}
		System.out.println();
	}

	static void test() throws Exception
	{
		HashMap diskElements = new MyHashMap();

		for (int i = 0; i < 200000; i++)
		{
			diskElements.put(new String("ddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
					+ i), new String("dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"));
		}
		System.out.println("-----------------------------");
		diskElements.clear();

		Thread.sleep(2 * 1000);
		System.out.println("-----------------------------清理");
		diskElements.clear();
		System.out.println("-----------------------------清理2");
		diskElements.clear();
		diskElements = null;
		System.gc();
		System.out.println("-----------------------------");
		Thread.sleep(2 * 1000);
		diskElements = null;
		diskElements = new MyHashMap();
		diskElements = null;
	}
}
