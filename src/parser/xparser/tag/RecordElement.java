package parser.xparser.tag;

public class RecordElement extends Tag {

	private RuleElement matchRule;

	private RuleElement digRule;

	private String file;

	private String ownerName;

	public RecordElement(RuleElement matchRule, RuleElement digRule) {
		super("record");
		this.matchRule = matchRule;
		this.digRule = digRule;
	}

	public RuleElement getMatchRule() {
		return matchRule;
	}

	public void setMatchRule(RuleElement matchRule) {
		this.matchRule = matchRule;
	}

	public RuleElement getDigRule() {
		return digRule;
	}

	public void setDigRule(RuleElement digRule) {
		this.digRule = digRule;
	}

	public String getFile() {
		return file;
	}

	public void setFile(String file) {
		this.file = file;
	}

	@Override
	public Object apply(Object params) {

		return digRule.apply(params);
	}

	public String getOwnerName() {
		return ownerName;
	}

	public void setOwnerName(String ownerName) {
		this.ownerName = ownerName;
	}
}
