package parser.xparser.tag;

public abstract class Tag {

	private String name;

	private Tag[] child;

	public Tag() {
		super();
	}

	public Tag(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Tag[] getChild() {
		return child;
	}

	public void setChild(Tag[] child) {
		this.child = child;
	}

	public boolean hasChild() {
		return child != null && child.length > 0;
	}

	public abstract Object apply(Object params);

}
