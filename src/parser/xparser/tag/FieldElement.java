package parser.xparser.tag;

public class FieldElement extends Tag {

	private int index;

	private PropertyElement property;

	private String ownerName;

	public FieldElement(int index, PropertyElement property, String ownerName) {
		super("field");
		this.index = index;
		this.property = property;
		this.ownerName = ownerName;
	}

	public FieldElement(int index) {
		super("field");
		this.index = index;
	}

	public int getIndex() {
		return index;
	}

	public void setIndex(int index) {
		this.index = index;
	}

	public PropertyElement getProperty() {
		return property;
	}

	public void setProperty(PropertyElement property) {
		this.property = property;
	}

	public String getOwnerName() {
		return ownerName;
	}

	public void setOwnerName(String ownerName) {
		this.ownerName = ownerName;
	}

	/**
	 * @param 接受的参数只能是数组
	 */
	@Override
	public Object apply(Object params) {
		String result = null;

		if (params != null) {
			if (!params.getClass().isArray()) {
				params = new String[]{params.toString()};
			}

			String[] strFields = (String[]) params;
			result = strFields[index];
			if (property != null) {
				property.setValue(result);
			}

			if (this.hasChild()) {
				Tag[] childs = this.getChild();
				for (Tag child : childs) {
					child.apply(result);
				}
			}
		}
		return result;
	}

}
