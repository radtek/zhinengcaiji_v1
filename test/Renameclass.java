import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Renameclass
{
	public static void main(String[] args) throws InterruptedException
	{
		// File dir = new
		// File("C:\\Users\\ChenSijiang\\Desktop\\eric_log\\gudusoft\\gsqlparser");
		// handle(dir);
		CountDownLatch cd = new CountDownLatch(3);
		System.err.println(cd.getCount());
		cd.countDown();
		System.err.println(cd.getCount());
		cd.countDown();
		System.err.println(cd.getCount());
		cd.await();
		System.err.println(cd.getCount());
		cd.countDown();
		System.err.println(cd.getCount());

	}

	static boolean handle(File dir)
	{
		File[] subs = dir.listFiles();
		boolean hasDir = false;
		for (File f : subs)
		{
			if ( f.isDirectory() )
			{
				hasDir = true;
				return handle(f);
			}
			else
			{
				if ( f.getName().endsWith(".class") )
					f.delete();
				else if ( f.getName().endsWith(".jad") )
					f.renameTo(new File(f.getAbsolutePath().replace(".jad", ".java")));
			}
		}
		return hasDir;
	}
}
