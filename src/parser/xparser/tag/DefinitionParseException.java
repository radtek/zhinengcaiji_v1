package parser.xparser.tag;

public class DefinitionParseException extends Exception {

	private static final long serialVersionUID = -4340720184724989386L;

	public DefinitionParseException(String msg, Throwable t) {
		super(msg, t);
	}

	public DefinitionParseException(String msg) {
		super(msg);
	}

	public DefinitionParseException(Throwable t) {
		super(t);
	}
}
