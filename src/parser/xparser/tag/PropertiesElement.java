package parser.xparser.tag;

public class PropertiesElement extends Tag {

	private String table;

	private boolean autoCreateTable = false;

	private boolean store = true; // 是否把数据存储到库中,默认为true

	public PropertiesElement(String table) {
		super("properties");
		this.table = table;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public PropertyElement getPropertyById(int id) {
		Tag[] tags = getChild();
		for (Tag tag : tags) {
			if (((PropertyElement) tag).getId() == id) {
				return (PropertyElement) tag;
			}
		}
		return null;
	}

	@Override
	public Object apply(Object params) {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean isAutoCreateTable() {
		return autoCreateTable;
	}

	public void setAutoCreateTable(boolean autoCreateTable) {
		this.autoCreateTable = autoCreateTable;
	}

	public boolean isStore() {
		return store;
	}

	public void setStore(boolean store) {
		this.store = store;
	}

}
