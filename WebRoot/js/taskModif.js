$(document).ready(function() {
	$("#back").click(function() {
		window.location.replace($("#returnUrl").val());
	});
	$("#save").click(function() {
		if (checkTask(false)) {
			$("#modifForm").submit();
		}
	});
	var ct = $("#collectType").val();
	if (ct != "") {
		$("select[name=collectType]").find("option").each(function() {
			if (parseInt($(this).val()) == parseInt(ct)) {
				$(this).attr("selected", true);
			}
		});
	}
	var cp = $("#collectPeriod").val();
	if (cp != "") {
		$("select[name=collectPeriod]").find("option").each(function() {
			if (parseInt($(this).val()) == parseInt(cp)) {
				$(this).attr("selected", true);
			}
		});
	}
});