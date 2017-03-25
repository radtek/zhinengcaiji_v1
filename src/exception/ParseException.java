package exception;

/**
 * 解析异常基类
 * 
 * @author YangJian
 * @since 1.0
 */
public class ParseException extends Exception {

	private static final long serialVersionUID = -4607404540270646338L;

	public ParseException() {
		super();
	}

	public ParseException(String message, Throwable cause) {
		super(message, cause);
	}

	public ParseException(String message) {
		super(message);
	}

	public ParseException(Throwable cause) {
		super(cause);
	}

}
