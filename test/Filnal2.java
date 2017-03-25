import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

class Fxxx extends FileInputStream
{
	public Fxxx(String name) throws FileNotFoundException
	{
		super(name);
	}

	static FileInputStream xxxx;

	@Override
	protected void finalize()
	{
		xxxx = this;
	}

}

public class Filnal2
{

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException
	{
		try
		{
			Fxxx fffff = new Fxxx("c:\\asdflskdjflsdajfsad.exx");
			System.out.println(fffff);
		}
		catch (Exception e)
		{
		//	e.printStackTrace();
		}
		System.gc();
		System.runFinalization();

		System.out.println("Foo的实例：" + Fxxx.xxxx);
		Fxxx.xxxx.read();
		Fxxx.xxxx.close();

	}

}
