//http://stackoverflow.com/questions/1144783/replacing-all-occurrences-of-a-string-in-javascript
String.prototype.replaceAll = function (find, replace) {
	var str = this;
	return str.replace(new RegExp(find.replace(/[-\/\\^$*+?.()|[\]{}]/g, '\\$&'), 'g'), replace);
};

$(function() {
	//global ajax vars
	window.setRequest = null;
	window.dataAmountRequest = null;
	window.sourceRequest = null;
	window.filesystemRequest = null;
	window.dataSummaryTableRequest = null;

	$('#data_amount_table').html('<img src="/static/images/ajax-loader.gif" class="loading"/>');

	window.dataAmountRequest = $.ajax({
		type: 'GET',
		url: '/data_amount',
		success: function(data) {
			$('#data_amount_table').html(data);
		},
		dataType: 'html'
	});

	$('#source_table').html('<img src="/static/images/ajax-loader.gif" class="loading"/>');

	window.sourceRequest = $.ajax({
		type: 'GET',
		url: '/source_table',
		success: function(data) {
			$('#source_table').html(data);
		},
		dataType: 'html'
	});

	$('#filesystem_table').html('<img src="/static/images/ajax-loader.gif" class="loading"/>');

	window.filesystemRequest = $.ajax({
		type: 'GET',
		url: '/filesystem',
		success: function(data) {
			$('#filesystem_table').html(data);
		},
		dataType: 'html'
	});

	getComments();
});

function abortRequestIfPending(request) {
	if (request) {
		request.abort();
		return null;
	}
	return request;
};

function getComments() {
	$('#comments_div').html('<img src="/static/images/ajax-loader.gif" class="loading"/>');

	$.ajax({
		type: 'GET',
		url: '/get_all_comments',
		success: function(data) {
			$('#comments_div').html(data);
			$('#comments_list').collapsible({
				animate: false
			});
		},
		dataType: 'html'
	});
};
