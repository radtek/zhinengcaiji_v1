package framework.ui;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;

import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;

/**
 * <p>
 * IGP配置窗口。
 * <p>
 * 
 * @author ChenSijiang 2010-11-8
 * @since 1.2
 */
public class ConfigWindow extends ModalDialog {

	private static final long serialVersionUID = 7379732434615834003L;

	private JPanel content = new JPanel(new FlowLayout(FlowLayout.LEFT));

	private JButton btnSubmit = new JButton("提交");

	private JFileChooser fcCurrentpath = new JFileChooser(new File("." + File.separator));

	private JTextField txtCurrentPath = new JTextField(20);

	private JButton btnCurrentPath = new JButton("选择...");

	private JLabel lbCurrentpath = new JLabel("      临时目录");

	private JTextField txtTempletpath = new JTextField(20);

	private JButton btnTempletpath = new JButton("选择...");

	private JLabel lbTempletpath = new JLabel("      模板目录");

	private JLabel lbTelnetPort = new JLabel("TELNET端口");

	private JTextField txtTelnetPort = new JTextField(20);

	private JLabel lbWinrar = new JLabel("WinRar目录");

	private JTextField txtWinrar = new JTextField(20);

	private JButton btnWinrar = new JButton("选择...");

	public ConfigWindow(Window parent) {
		super(parent);
		setSize(400, 600);
		setTitle("IGP配置");
		addComps();
		initEvents();
	}

	private void addComps() {
		fcCurrentpath.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
		fcCurrentpath.setDialogTitle("请选择目录");
		add(btnSubmit, BorderLayout.SOUTH);
		add(content, BorderLayout.CENTER);
		content.add(lbCurrentpath);
		content.add(txtCurrentPath);
		content.add(btnCurrentPath);
		content.add(lbTempletpath);
		content.add(txtTempletpath);
		content.add(btnTempletpath);
		content.add(lbTelnetPort);
		content.add(txtTelnetPort);
		content.add(new JLabel("      "));
		content.add(lbWinrar);
		content.add(txtWinrar);
		content.add(btnWinrar);
	}

	private void initEvents() {
		btnCurrentPath.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(ActionEvent e) {
				int ret = fcCurrentpath.showDialog(ConfigWindow.this, "确定");
				if (ret == JFileChooser.APPROVE_OPTION) {
					txtCurrentPath.setText(fcCurrentpath.getSelectedFile().getAbsolutePath());
				}
			}
		});
		btnTempletpath.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(ActionEvent e) {
				int ret = fcCurrentpath.showDialog(ConfigWindow.this, "确定");
				if (ret == JFileChooser.APPROVE_OPTION) {
					txtTempletpath.setText(fcCurrentpath.getSelectedFile().getAbsolutePath());
				}
			}
		});
		btnWinrar.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(ActionEvent e) {
				int ret = fcCurrentpath.showDialog(ConfigWindow.this, "确定");
				if (ret == JFileChooser.APPROVE_OPTION) {
					txtWinrar.setText(fcCurrentpath.getSelectedFile().getAbsolutePath());
				}
			}
		});
	}
}
