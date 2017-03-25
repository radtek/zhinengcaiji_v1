package util;

/**
 * 操作结果 基类
 * 
 * @author YangJian
 * @since 1.0
 */
public class Result {

	/** 操作结果码 */
	private int code;

	/** 操作结果详细信息 */
	private String detailMessage;

	/** 产生这个结果的原因 */
	private Result cause = this;

	public Result(int code) {
		super();
		this.code = code;
	}

	public Result(int code, String message) {
		super();
		this.code = code;
		this.detailMessage = message;
	}

	public Result(int code, String message, Result cause) {
		super();
		this.code = code;
		this.detailMessage = message;
		this.cause = cause;
	}

	public Result(Result cause) {
		super();
		this.code = (cause == null ? 0 : cause.getCode());
		this.detailMessage = (cause == null ? null : cause.toString());
		this.cause = cause;
	}

	public int getCode() {
		return code;
	}

	public void setCode(int code) {
		this.code = code;
	}

	public String getMessage() {
		return detailMessage;
	}

	public void setMessage(String message) {
		this.detailMessage = message;
	}

	public Result getCause() {
		return (cause == this ? null : cause);
	}

	public void setCause(Result cause) {
		this.cause = cause;
	}

	@Override
	public String toString() {
		String s = getClass().getName();
		int code = getCode();
		String message = getMessage();
		return (message != null) ? (s + ":" + code + ": " + message) : s + ":" + code;
	}

}
