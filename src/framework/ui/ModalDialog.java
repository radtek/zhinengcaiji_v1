package framework.ui;

import java.awt.Window;

import javax.swing.JDialog;

/**
 * 模式对话框。
 * 
 * @author ChenSijiang 2010-11-8
 * @version 1.2
 */
public class ModalDialog extends JDialog {

	private static final long serialVersionUID = -67123799398619652L;

	// 父窗口
	protected Window parent;

	/**
	 * 构造模式对话框，指定父窗口。
	 * 
	 * @param parent
	 *            父窗口
	 */
	public ModalDialog(Window parent) {
		super(parent);
		this.parent = parent;
		setResizable(false);
		setModal(true);
	}

	@Override
	public void setVisible(boolean b) {
		setLocation(parent.getX() + (parent.getWidth() - getWidth()) / 2, parent.getY() + (parent.getHeight() - getHeight()) / 2);
		super.setVisible(b);
	}
}
