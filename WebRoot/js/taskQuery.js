$(document).ready(function() {
	// ����
		$("#back").click(function() {
			window.history.go(-1);
		});
		// ��ѯ
		$("#query").click(function() {
			$("form:first").submit();
		});
	});