package util.loganalyzer;

/**
 * logAnalyzer 异常类
 * 
 * @author liuwx
 * 
 */
public class LogAnalyzerException extends Exception {

	private static final long serialVersionUID = -6141273162666604535L;

	public LogAnalyzerException() {
		super();
	}

	public LogAnalyzerException(String message, Throwable cause) {
		super(message, cause);
	}

	public LogAnalyzerException(String message) {

		super(message);
	}

	public LogAnalyzerException(Throwable cause) {
		super(cause);
	}

}
