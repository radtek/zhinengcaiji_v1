package access;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

import sun.misc.Unsafe;

public class TestCallSqlldr
{  private static final Unsafe unsafe = Unsafe.getUnsafe();
	public static void main(String[] args) throws Exception
	{
		// Process p = Runtime.getRuntime().exec("sqlldr");

		// System.out.println(p.waitFor());
		// System.out.println(p.exitValue());
		
		
		AtomicInteger ai = new AtomicInteger(0);
		System.out.println(ai.getAndIncrement());
		System.out.println(ai.getAndIncrement());
		System.out.println(ai.getAndIncrement());
		AtomicIntegerArray ar = new AtomicIntegerArray(1); 
	}
}
