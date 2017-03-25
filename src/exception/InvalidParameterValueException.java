package exception;

/**
 * 无效的参数取值异常
 * 
 * @author YangJian
 * @since 1.0
 */
public class InvalidParameterValueException extends Exception {

	private static final long serialVersionUID = -4221202827734871368L;

	public InvalidParameterValueException() {
		super();
	}

	public InvalidParameterValueException(String message, Throwable cause) {
		super(message, cause);
	}

	public InvalidParameterValueException(String message) {
		super(message);
	}

	public InvalidParameterValueException(Throwable cause) {
		super(cause);
	}

}
