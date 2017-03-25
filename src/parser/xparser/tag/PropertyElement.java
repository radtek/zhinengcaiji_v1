package parser.xparser.tag;

public class PropertyElement extends Tag {

	private int id;

	private String alias;

	private String column;

	private String value;

	private String var; // 保存外界变量

	private boolean varFlag = false; // 是否为外界变量，默认为false

	private String type;

	private String format;

	private String tableName;

	private String scope; // 范围，值：global

	public PropertyElement() {
		super("property");
	}

	public PropertyElement(int id, String alias, String column) {
		this();
		this.id = id;
		this.alias = alias;
		this.column = column;
	}

	public PropertyElement(int id, String alias, String column, String type, String format) {
		this(id, alias, column);
		this.type = type;
		this.format = format;
	}

	public PropertyElement(int id, String alias, String column, String type, String format, String scope) {
		this(id, alias, column, type, format);
		this.scope = scope;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getAlias() {
		return alias;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public void setAlias(String alias) {
		this.alias = alias;
	}

	public String getColumn() {
		return column;
	}

	public void setColumn(String column) {
		this.column = column;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public boolean isVar() {
		return varFlag;
	}

	public void setVarFlag(boolean varFlag) {
		this.varFlag = varFlag;
	}

	public String getVar() {
		return var;
	}

	public void setVar(String var) {
		this.var = var;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getFormat() {
		return format;
	}

	public void setFormat(String format) {
		this.format = format;
	}

	public String getScope() {
		return scope;
	}

	public void setScope(String scope) {
		this.scope = scope;
	}

	/**
	 * 接收字符串
	 */
	@Override
	public Object apply(Object params) {
		// if ( params!=null )
		// {
		// String s = params.toString();
		// }
		return getValue();

	}

}
