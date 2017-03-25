$(document).ready(function() {
	$("#back").click(function() {
		window.location.replace($("#returnUrl").val());
	});
});