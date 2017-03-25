package framework;

public class SystemConfigException extends Exception {

	private static final long serialVersionUID = 7913349358471652427L;

	public SystemConfigException(String message) {
		super(message);
	}

	public SystemConfigException(String message, Throwable cause) {
		super(message, cause);
	}
}
