package parser.lucent.evdo;

/**
 * Evdo话单解析异常。
 * 
 * @author ChenSijiang 2009.01.25
 * @since 1.0
 */
public class EvdoParseException extends Exception {

	private static final long serialVersionUID = -2417956722452853946L;

	public EvdoParseException() {
		super();
	}

	public EvdoParseException(String message) {
		super(message);
	}

	public EvdoParseException(Throwable cause) {
		super(cause);
	}

	public EvdoParseException(String message, Throwable cause) {
		super(message, cause);
	}

}
