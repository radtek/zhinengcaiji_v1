package framework.ui;

import javax.swing.JComponent;

/**
 * <p>
 * IGP配置表单中的一个字段。
 * <p>
 * 
 * @author ChenSijiang 2010-11-8
 * @since 1.2
 */
public interface ConfigField {

	/**
	 * <p>
	 * 验证表单的输入。
	 * </p>
	 * 
	 * @return 如果验证通过，返回<code>null</code>，否则，返回相应的错误信息
	 */
	String validate();

	/**
	 * <p>
	 * 获取对应的{@link JComponent}对象。
	 * </p>
	 * <p>
	 * 获取到的{@link JComponent}对象，包括了一个标签，和一个输入控件。
	 * </p>
	 * 
	 * @return {@link JComponent}对象
	 */
	JComponent getFieldCompnent();
}
