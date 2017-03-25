package framework.ui;

import java.awt.Toolkit;

import javax.swing.JFrame;

/**
 * <p>
 * IGP的窗口基类，提供初始化窗口大小、窗口位置等基本属性。
 * <p>
 * 
 * @author ChenSijiang 2010-11-8
 * @since 1.2
 */
public abstract class BaseWindow extends JFrame {

	private static final long serialVersionUID = 3840461744414857260L;

	private static final int INIT_WIDTH = 320;

	private static final int INIT_HEIGH = 120;

	/**
	 * <p>
	 * 构造方法。
	 * </p>
	 * 
	 * @throws Exception
	 *             抛出所有异常
	 */
	public BaseWindow() throws Exception {
		setSize(INIT_WIDTH, INIT_HEIGH);
		setDefaultCloseOperation(DISPOSE_ON_CLOSE);
		Toolkit toolkit = Toolkit.getDefaultToolkit();
		int x = toolkit.getScreenSize().width / 2 - getWidth() / 2;
		int y = toolkit.getScreenSize().height / 2 - getHeight() / 2;
		setLocation(x, y);
	}
}
