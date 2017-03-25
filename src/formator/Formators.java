package formator;

/**
 * 默认通用的formator集合
 * 
 * @author liangww
 * @version 1.0.0 1.0.1 liangww 2012-07-17 删除FormatorMap方法，并把其相关东西移到pbean<br>
 * 
 * @create 2012-7-17 上午09:38:14
 */
public abstract class Formators {

	/**
	 * DefualtFormator 什么都不做
	 * 
	 * @author liangww
	 * @version 1.0
	 * @create 2012-7-15 下午03:20:17
	 */
	static public class DefaultFormator implements Formator {

		@Override
		public String format(String str) {
			// TODO Auto-generated method stub
			return str;
		}
	}

	/**
	 * NumberFormator
	 * 
	 * @author liangww
	 * @version 1.0
	 * @create 2012-7-15 下午03:20:17
	 */
	static public class NumberFormator implements Formator {

		@Override
		public String format(String str) {
			// TODO Auto-generated method stub
			// 160113835
			return Long.valueOf(str).toString();
		}
	}

}
