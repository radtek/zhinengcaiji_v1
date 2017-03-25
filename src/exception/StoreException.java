package exception;

/**
 * 存储模块 异常基类
 * 
 * @author YangJian
 * @since 3.1
 */
public class StoreException extends Exception {

	private static final long serialVersionUID = -4972255405605742292L;

	public StoreException() {
		super();
	}

	public StoreException(String message, Throwable cause) {
		super(message, cause);
	}

	public StoreException(String message) {
		super(message);
	}

	public StoreException(Throwable cause) {
		super(cause);
	}

}
