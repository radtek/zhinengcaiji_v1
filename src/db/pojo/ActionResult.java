package db.pojo;

import framework.IGPError;

/**
 * 操作结果
 * 
 * @author IGP TDT
 * @since 1.0
 */
public class ActionResult {

	private IGPError error;

	private String forwardURL; // forward页面地址，由开发人员具体指定

	private String returnURL; // 返回页面,为result.jsp中返回链接的地址

	private Object data; // 数据

	private Object wparam; // 附加参数1,返回给前台页面的附加参数

	private Object lparam; // 附加参数2,返回给前台页面的附加参数

	/**
	 * 在JAVA BEAN的方法getXyz/setXyz即对应名为xyz的JavaBean属性，这也就是通过去掉 get 并将第一个字符变成小写之后得到的。 注意，一个例外是如果方法名的“get/set”之后的头两个字符都是大写的，其对应JavaBean属性的名称保持不变。
	 * 即：getABC()/setABC(...) 对应属性名应为ABC。这是考虑属性名全为大写字母的属性而设计的。
	 * 
	 * SUN官方的java beans的规范文档关于方法名get/set后接双大写字母的说明，COPY如下：
	 * 
	 * Capitalization of inferred names.
	 * 
	 * When we use design patterns to infer a property or event name,we need to decide what rules to follow for capitalizing the inferred name.If we
	 * extract the name from the middle of a normal mixedCase style Java name then the name will, by default, begin with a capital letter. Java
	 * programmers are accustomed to having normal identifiers start with lower case letters. Vigorous reviewer input has convinced us that we should
	 * follow this same conventional rule for property and event names. Thus when we extract a property or event name from the middle of an existing
	 * Java name,we normally convert the first character to lower case.However to support the occasional use of all upper-case names,we check if the
	 * first two characters of the name are both upper case and if so leave it alone. So for example, “FooBah” becomes “fooBah” “Z” becomes “z” “URL”
	 * becomes “URL” We provide a method Introspector.decapitalize which implements this conversion rule.
	 * */

	public ActionResult(IGPError error, String forwordURL, String returnURL, Object data) {
		super();
		this.error = error;
		this.forwardURL = forwordURL;
		this.returnURL = returnURL;
		this.data = data;
	}

	public ActionResult() {
		super();
	}

	public IGPError getError() {
		return error;
	}

	public void setError(IGPError error) {
		this.error = error;
	}

	public String getForwardURL() {
		return forwardURL;
	}

	public void setForwardURL(String forwardURL) {
		this.forwardURL = forwardURL;
	}

	public Object getData() {
		return data;
	}

	public void setData(Object data) {
		this.data = data;
	}

	public String getReturnURL() {
		return returnURL;
	}

	public void setReturnURL(String returnURL) {
		this.returnURL = returnURL;
	}

	public Object getWparam() {
		return wparam;
	}

	public void setWparam(Object wparam) {
		this.wparam = wparam;
	}

	public Object getLparam() {
		return lparam;
	}

	public void setLparam(Object lparam) {
		this.lparam = lparam;
	}

}
