package util.exception;

/**
 * @author yuy 数据库连接为空异常
 */
public class NullDBConnException extends RuntimeException {

	private static final long serialVersionUID = 5162710183389028799L;

	public NullDBConnException() {
	}

	public NullDBConnException(String message) {
		super(message);
	}

	public NullDBConnException(Throwable cause) {
		super(cause);
	}

	public NullDBConnException(String message, Throwable cause) {
		super(message, cause);
	}

}
