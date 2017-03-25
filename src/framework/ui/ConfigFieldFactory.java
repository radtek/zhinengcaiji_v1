package framework.ui;

import javax.swing.JFrame;

/**
 * <p>
 * IGP配置表单字段工厂。
 * <p>
 * 
 * @author ChenSijiang 2010-11-8
 * @since 1.2
 */
public class ConfigFieldFactory {

	protected JFrame parent;

	/**
	 * <p>
	 * 构造方法，指定表单所属的父窗体。
	 * </p>
	 * 
	 * @param parent
	 *            表单所属的父窗体
	 */
	public ConfigFieldFactory(JFrame parent) {
		this.parent = parent;
	}
}
