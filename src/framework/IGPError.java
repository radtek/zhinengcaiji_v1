package framework;

/**
 * IGP系统中错误类
 * 
 * @author IGP TDT
 * @since 1.0
 */
public class IGPError {

	private String code;

	private String des;

	private String cause;

	private String action;

	public IGPError() {
		super();
	}

	public IGPError(String code, String des, String cause, String action) {
		super();
		this.code = code;
		this.des = des;
		this.cause = cause;
		this.action = action;
	}

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	public String getDes() {
		return des;
	}

	public void setDes(String des) {
		this.des = des;
	}

	public String getCause() {
		return cause;
	}

	public void setCause(String cause) {
		this.cause = cause;
	}

	public String getAction() {
		return action;
	}

	public void setAction(String action) {
		this.action = action;
	}
}
