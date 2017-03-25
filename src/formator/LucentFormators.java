package formator;

/**
 * 朗讯的formator
 * 
 */
public abstract class LucentFormators {

	/**
	 * SmartPmDscFormator
	 * 
	 * @author liangww
	 * @version 1.0
	 * @create 2012-7-15 下午03:20:17
	 */
	public static class SmartPmDscFormator implements Formator {

		@Override
		public String format(String str) {
			// TODO Auto-generated method stub
			// 160113835 只取16
			return str.substring(0, 2);
		}
	}

	/**
	 * SmartPmEcpFormator
	 * 
	 * @author liangww
	 * @version 1.0
	 * @create 2012-7-15 下午03:20:17
	 */
	public static class SmartPmEcpFormator implements Formator {

		@Override
		public String format(String str) {
			// TODO Auto-generated method stub
			// 160113835 只取01
			if(str.length() > 3){
				return str.substring(2, 4);
			}
			return null;
		}
	}

	/**
	 * SmartPmSidFormator
	 * 
	 * @author liangww
	 * @version 1.0
	 * @create 2012-7-15 下午03:20:17
	 */
	public static class SmartPmSidFormator implements Formator {

		@Override
		public String format(String str) {
			// TODO Auto-generated method stub
			// 160113835 只取13835
			if(str.length() > 4){
				return str.substring(4);
			}else{
				return "";
			}
			
		}
	}

	/**
	 * VersionFormator
	 * 
	 * @author liangww
	 * @version 1.0
	 * @create 2012-7-15 下午03:20:17
	 */
	static public class SmartPmVersionFormator implements Formator {

		@Override
		public String format(String str) {
			// TODO Auto-generated method stub
			// r36.0 只取26.0
			return str.substring(1);
		}
	}

	/**
	 * SmartPmCellFormator
	 * 
	 * @author liangww
	 * @version 1.0
	 * @create 2012-7-15 下午03:20:17
	 */
	static public class SmartPmCellFormator implements Formator {

		@Override
		public String format(String str) {
			// TODO Auto-generated method stub
			// CL0000 取0000转成int ，就是0
			return Integer.valueOf(str.substring(2)).toString();
		}
	}

}
