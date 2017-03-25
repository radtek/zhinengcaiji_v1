package parser.corba.nms;

import java.io.IOException;

public class IorNotFoundException extends IOException {

	private static final long serialVersionUID = -8504658921118708971L;

	public IorNotFoundException() {
		super();
	}

	public IorNotFoundException(String message, Throwable cause) {
		super(message, cause);
	}

	public IorNotFoundException(String message) {
		super(message);
	}

	public IorNotFoundException(Throwable cause) {
		super(cause);
	}

}
