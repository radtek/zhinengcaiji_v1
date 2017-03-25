class Foo
{
	public Foo()
	{
		throw new NullPointerException();
	}
}

class Xoo extends Foo
{
	static Foo fx;

	public Xoo()
	{
	}

	@Override
	protected void finalize() throws Throwable
	{
		fx = this;
	}
}

public class Finalizezzzzz
{

	public static void main(String[] args)
	{
		try
		{
			Xoo fffff = new Xoo();
			System.out.println(fffff);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		System.gc();
		System.runFinalization();

		System.out.println("Foo的实例：" + Xoo.fx);
	}
}
