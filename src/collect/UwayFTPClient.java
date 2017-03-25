package collect;

import java.lang.reflect.Field;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPClientConfig;

/**
 * 继承自org.apache.commons.net.ftp.FTPClient，重写configure方法，使其在多次调用的情况下也能生效
 * 
 * @author ChenSijiang 2010-8-25
 */
public class UwayFTPClient extends FTPClient {

	@Override
	public void configure(FTPClientConfig config) {
		try {
			Class<?> cls = getClass().getSuperclass();
			Field fi = cls.getDeclaredField("__entryParser");
			fi.setAccessible(true);
			fi.set(this, null);
		} catch (Exception e) {
			e.printStackTrace();
		}
		super.configure(config);
	}
}
