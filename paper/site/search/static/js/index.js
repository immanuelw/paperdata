//http://stackoverflow.com/questions/1144783/replacing-all-occurrences-of-a-string-in-javascript
String.prototype.replaceAll = function (find, replace) {
	var str = this;
	return str.replace(new RegExp(find.replace(/[-\/\\^$*+?.()|[\]{}]/g, '\\$&'), 'g'), replace);
};

$(function() {
	var startDatePicker = $('#datepicker_start');
	var endDatePicker = $('#datepicker_end');
	startDatePicker.datetimepicker();
	endDatePicker.datetimepicker();

	if (startDatePicker.val().length > 0 && endDatePicker.val().length > 0) {
		// The date pickers have already been filled with values, which means we're viewing a set.
		// Nothing needs to be done.
	} else if (sessionStorage.startDate && sessionStorage.endDate) {
		startDatePicker.val(sessionStorage.startDate);
		endDatePicker.val(sessionStorage.endDate);
	} else {
		var now = new Date();
		var nowStr = getDateTimeString(now);

		var MS_PER_DAY = 86400000;
		var yesterday = new Date(now.getTime() - MS_PER_DAY);
		var yesterdayStr = getDateTimeString(yesterday);

		startDatePicker.val(yesterdayStr);
		endDatePicker.val(nowStr);

		sessionStorage.startDate = yesterdayStr;
		sessionStorage.endDate = nowStr;
	}

	var jd_startPicker = $('#jd_start');
	var jd_endPicker = $('#jd_end');

	if (jd_startPicker.val().length > 0 && jd_endPicker.val().length > 0) {
		// The date pickers have already been filled with values, which means we're viewing a set.
		// Nothing needs to be done.
	} else if (sessionStorage.jd_start && sessionStorage.jd_end) {
		jd_startPicker.val(sessionStorage.jd_start);
		jd_endPicker.val(sessionStorage.jd_end);
	} else {
		var nothing = '';
		jd_startPicker.val(nothing);
		jd_endPicker.val(nothing);

		sessionStorage.jd_start = nothing;
		sessionStorage.jd_end = nothing;
	}

	//global ajax vars
	window.setRequest = null;
	window.dataSummaryTableRequest = null;
	window.daySummaryTableRequest = null;
	window.obsTableRequest = null;
	window.fileTableRequest = null;

	// Set up the tabs.
	$('#tabs').tabs({
		beforeLoad: function(event, ui) {
			if (ui.tab.data('loaded')) {
				event.preventDefault();
				return;
			}

			ui.panel.html('<img src="/static/images/ajax-loader.gif" class="loading"/>');

			if (ui.ajaxSettings.url.search('&set=') === -1) { // There is no set, so we need to add the date range.
				var startTimeStr = $('#datepicker_start').val().replaceAll('/', '-').replaceAll(' ', 'T') + ':00Z';
				var endTimeStr = $('#datepicker_end').val().replaceAll('/', '-').replaceAll(' ', 'T') + ':00Z';
				ui.ajaxSettings.url += '&start=' + startTimeStr + '&end=' + endTimeStr;
			}

			ui.jqXHR.success(function() {
				ui.tab.data('loaded', true);
			});
		}
	});

	getObservations(false /* Don't load the first tab, it's already being loaded */);
});

function getDateTimeString(now) {
	var month = ('0' + (now.getUTCMonth() + 1)).slice(-2);
	var date = ('0' + now.getUTCDate()).slice(-2);
	var hours = ('0' + now.getUTCHours()).slice(-2);
	var minutes = ('0' + now.getUTCMinutes()).slice(-2);
	return now.getUTCFullYear() + '/' + month + '/' + date + ' ' + hours + ':' + minutes;
};

function abortRequestIfPending(request) {
	if (request) {
		request.abort();
		return null;
	}
	return request;
};

function saveTable(table) {
	window.saveTableRequest = abortRequestIfPending(window.saveTableRequest);
	var start = $('#datepicker_start').val();
	var end = $('#datepicker_end').val();
	re = /^\d{4}\/\d{2}\/\d{2} \d{2}:\d{2}$/;

	var jd_start = $('#jd_start').val();
	var jd_end = $('#jd_end').val();

	// Update the sessionStorage
	sessionStorage.startDate = start;
	sessionStorage.endDate = end;

	sessionStorage.jd_start = jd_start;
	sessionStorage.jd_end = jd_end;

	var startDate, endDate;

	if (start.match(re)) {
		startDate = getDate(start);
	} else {
		alert('Invalid datetime format: ' + start);
		return;
	}

	if (end.match(re)) {
		endDate = getDate(end);
	} else {
		alert('Invalid datetime format: ' + end);
		return;
	}

	var startUTC = startDate.toISOString().slice(0, 19) + 'Z';
	var endUTC = endDate.toISOString().slice(0, 19) + 'Z';

	var polarization = $('#polarization_dropdown').val();
	var era_type = $('#era_type_dropdown').val();
	var host = $('#host_dropdown').val();
	var filetype = $('#filetype_dropdown').val();

	if (table === 'obs') {
		window.saveTableRequest = $.ajax({
			type: 'POST',
			url: '/save_obs',
			data: {
				'starttime': startUTC,
				'endtime': endUTC,
				'jd_start': jd_start,
				'jd_end': jd_end,
				'polarization': polarization,
				'era_type': era_type,
			},
			success: function(data) {
				window.alert(JSON.stringify(data));
			},
			dataType: 'json'
		});
	} else if (table === 'files') {
		window.saveTableRequest = $.ajax({
			type: 'POST',
			url: '/save_files',
			data: {
				'starttime': startUTC,
				'endtime': endUTC,
				'jd_start': jd_start,
				'jd_end': jd_end,
				'host': host,
				'filetype': filetype,
			},
			success: function(data) {
				window.alert(JSON.stringify(data));
			},
			dataType: 'json'
		});
	} else {
		alert('Invalid json');
		return;
	}
};

function getObservations(loadTab) {
	window.dataSummaryTableRequest = abortRequestIfPending(window.dataSummaryTableRequest);
	window.daySummaryTableRequest = abortRequestIfPending(window.daySummaryTableRequest);
	window.obsTableRequest = abortRequestIfPending(window.obsTableRequest);
	window.fileTableRequest = abortRequestIfPending(window.fileTableRequest);

	var start = $('#datepicker_start').val();
	var end = $('#datepicker_end').val();
	re = /^\d{4}\/\d{2}\/\d{2} \d{2}:\d{2}$/;

	var jd_start = $('#jd_start').val();
	var jd_end = $('#jd_end').val();

	// Update the sessionStorage
	sessionStorage.startDate = start;
	sessionStorage.endDate = end;

	sessionStorage.jd_start = jd_start;
	sessionStorage.jd_end = jd_end;

	var startDate, endDate;

	if (start.match(re)) {
		startDate = getDate(start);
	} else {
		alert('Invalid datetime format: ' + start);
		return;
	}

	if (end.match(re)) {
		endDate = getDate(end);
	} else {
		alert('Invalid datetime format: ' + end);
		return;
	}

	$('#obs_table').html('<img src="/static/images/ajax-loader.gif" class="loading"/>');
	$('#file_table').html('<img src="/static/images/ajax-loader.gif" class="loading"/>');

	// Load the currently selected tab if it's not already being loaded.
	if (loadTab) {
		$('#tabs > ul > li').each(function(index) {
			$(this).data('loaded', false);
		});
		$('#tabs > ul > li > a').each(function(index) {
			var url = $(this).attr('href');
			var shortUrl = url.split('&').slice(0, 2).join('&');
			$(this).attr('href', shortUrl);
		});
		$('#tabs').tabs('load', $('#tabs').tabs('option', 'active'));
	}

	if (loadTab) { // The user pressed the 'Get observations' button, so they're viewing a date range now.
		$('#set_or_date_range_label').html('date range');
	}

	$('#data_summary_table').html('<img src="/static/images/ajax-loader.gif" class="loading"/>');
	$('#day_summary_table').html('<img src="/static/images/ajax-loader.gif" class="loading"/>');

	// Make each date into a string of the format 'YYYY-mm-ddTHH:MM:SSZ', which is the format used in the local database.
	var startUTC = startDate.toISOString().slice(0, 19) + 'Z';
	var endUTC = endDate.toISOString().slice(0, 19) + 'Z';

	var polarization = $('#polarization_dropdown').val();
	var era_type = $('#era_type_dropdown').val();
	var host = $('#host_dropdown').val();
	var filetype = $('#filetype_dropdown').val();

	window.obsTableRequest = $.ajax({
		type: 'POST',
		url: '/obs_table',
		data: {
			'starttime': startUTC,
			'endtime': endUTC,
			'jd_start': jd_start,
			'jd_end': jd_end,
			'polarization': polarization,
			'era_type': era_type,
		},
		success: function(data) {
			$('#obs_table').html(data);
		},
		dataType: 'html'
	});

	window.fileTableRequest = $.ajax({
		type: 'POST',
		url: '/file_table',
		data: {
			'starttime': startUTC,
			'endtime': endUTC,
			'jd_start': jd_start,
			'jd_end': jd_end,
			'host': host,
			'filetype': filetype,
		},
		success: function(data) {
			$('#file_table').html(data);
		},
		dataType: 'html'
	});

	window.dataSummaryTableRequest = $.ajax({
		type: 'POST',
		url: '/data_summary_table',
		data: {
			'starttime': startUTC,
			'endtime': endUTC,
			'jd_start': jd_start,
			'jd_end': jd_end,
		},
		success: function(data) {
			$('#data_summary_table').html(data);
		},
		dataType: 'html'
	});

	window.daySummaryTableRequest = $.ajax({
		type: 'POST',
		url: '/day_summary_table',
		data: {
			'starttime': startUTC,
			'endtime': endUTC,
			'jd_start': jd_start,
			'jd_end': jd_end,
		},
		success: function(data) {
			$('#day_summary_table').html(data);
		},
		dataType: 'html'
	});
};

function getDate(datestr) {
	var year = datestr.substring(0, 4);
	var month = datestr.substring(5, 7);
	var day = datestr.substring(8, 10);
	var hour = datestr.substring(11, 13);
	var minute = datestr.substring(14, 16);
	return new Date(Date.UTC(year, month - 1, day, hour, minute, 0));
};
