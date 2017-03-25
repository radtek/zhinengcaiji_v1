package web.server;

import java.util.Map;

/**
 * HttpServer接口
 * 
 * @author ltp
 * @since 1.0
 */
public interface HttpServer {

	public boolean start() throws Exception;

	public boolean stop() throws Exception;

	public boolean restart() throws Exception;

	public void init(Map<String, String> params);
}
