package parser.xparser;

/**
 * 非法标签异常
 * 
 * @author ChenSijiang
 * @since 1.0
 */
public class IllegalTagException extends Exception {

	private static final long serialVersionUID = -7435669314371760771L;

	public IllegalTagException() {
		super();
	}

	public IllegalTagException(String message) {
		super(message);
	}

	public IllegalTagException(Throwable cause) {
		super(cause);
	}

	public IllegalTagException(String message, Throwable cause) {
		super(message, cause);
	}

}
