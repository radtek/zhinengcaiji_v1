package parser.xparser.tag;

public class RuleElement extends Tag {

	private int id;

	private String alias;

	public RuleElement(int id, String name) {
		super("rule");
		this.setName("rule");
		this.id = id;
		this.alias = name;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getName() {
		return alias;
	}

	public void setName(String alias) {
		this.alias = alias;
	}

	/**
	 * 
	 */
	@Override
	public Object apply(Object params) {
		Object result = null;
		if (params != null && this.hasChild()) {
			Tag[] childs = this.getChild();
			for (Tag child : childs) {
				result = child.apply(params);
			}
		}
		return result;
	}
}
