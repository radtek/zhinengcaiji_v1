package parser.eric.pm.parser;

/**
 * XmlAsTextHandler
 * 
 * @author YangJian
 * @since 1.0
 */
public interface XmlAsTextHandler {

	public void startDocument() throws Exception;

	public void startElement(String qName) throws Exception;

	public void content(String content) throws Exception;

	public void endElement(String qName) throws Exception;

	public void endDocument() throws Exception;
}
