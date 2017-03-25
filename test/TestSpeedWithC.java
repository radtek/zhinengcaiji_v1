import java.io.PrintWriter;

public class TestSpeedWithC
{
	public static void main(String[] args) throws Exception
	{

		PrintWriter pw = new PrintWriter("d:\\dbout.txt");
		long curr = System.currentTimeMillis();
		for (int i = 0; i < 9999999; i++)
		{
			pw.println(i);
		}
		long end = System.currentTimeMillis();
		System.out.println((end - curr) / 1000.0);

	}
}
