//http://stackoverflow.com/questions/1144783/replacing-all-occurrences-of-a-string-in-javascript
String.prototype.replaceAll = function (find, replace) {
    var str = this;
    return str.replace(new RegExp(find.replace(/[-\/\\^$*+?.()|[\]{}]/g, '\\$&'), 'g'), replace);
};

$(function() {
    var startDatePicker = $("#datepicker_start");
    var endDatePicker = $("#datepicker_end");
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

    //global ajax vars
    window.setRequest = null;
    window.dataAmountRequest = null;
    window.dataSummaryTableRequest = null;

    $("#data_amount_table").html("<img src='/static/images/ajax-loader.gif' class='loading'/>");

    window.dataAmountRequest = $.ajax({
        type: "GET",
        url: "/data_amount",
        success: function(data) {
            $("#data_amount_table").html(data);
        },
        dataType: "html"
    });

    // Set up the tabs.
    $("#tabs").tabs({
        beforeLoad: function(event, ui) {
            if (ui.tab.data("loaded")) {
                event.preventDefault();
                return;
            }

            ui.panel.html("<img src='/static/images/ajax-loader.gif' class='loading'/>");

            if (ui.ajaxSettings.url.search("&set=") === -1) { // There is no set, so we need to add the date range.
                var startTimeStr = $("#datepicker_start").val().replaceAll("/", "-").replaceAll(" ", "T") + ":00Z";
                var endTimeStr = $("#datepicker_end").val().replaceAll("/", "-").replaceAll(" ", "T") + ":00Z";
                ui.ajaxSettings.url += "&start=" + startTimeStr + "&end=" + endTimeStr;
            }

            ui.jqXHR.success(function() {
                ui.tab.data("loaded", true);
            });
        }
    });

    $("#filter_dropdown_div").html("<img src='/static/images/ajax-loader.gif' class='loading'/>");

    $.ajax({
        type: "GET",
        url: "/get_filters",
        success: function(data) {
            $("#filter_dropdown_div").html(data);
            applyFiltersAndSort();
        },
        dataType: "html"
    });

    getObservations(false /* Don't load the first tab, it's already being loaded */);
    getComments();
});

function getDateTimeString(now) {
    var month = ("0" + (now.getUTCMonth() + 1)).slice(-2);
    var date = ("0" + now.getUTCDate()).slice(-2);
    var hours = ("0" + now.getUTCHours()).slice(-2);
    var minutes = ("0" + now.getUTCMinutes()).slice(-2);
    return now.getUTCFullYear() + "/" + month + "/" + date + " " + hours + ":" + minutes;
};

function abortRequestIfPending(request) {
    if (request) {
        request.abort();
        return null;
    }
    return request;
};

function getObservations(loadTab) {
    window.dataSummaryTableRequest = abortRequestIfPending(window.dataSummaryTableRequest);

    var start = $("#datepicker_start").val();
    var end = $("#datepicker_end").val();
    re = /^\d{4}\/\d{2}\/\d{2} \d{2}:\d{2}$/;

    // Update the sessionStorage
    sessionStorage.startDate = start;
    sessionStorage.endDate = end;

    var startDate, endDate;

    if (start.match(re)) {
        startDate = getDate(start);
    } else {
        alert("Invalid datetime format: " + start);
        return;
    }

    if (end.match(re)) {
        endDate = getDate(end);
    } else {
        alert("Invalid datetime format: " + end);
        return;
    }

    // Load the currently selected tab if it's not already being loaded.
    if (loadTab) {
        $("#tabs > ul > li").each(function(index) {
            $(this).data("loaded", false);
        });
        $("#tabs > ul > li > a").each(function(index) {
            var url = $(this).attr("href");
            var shortUrl = url.split("&").slice(0, 2).join("&");
            $(this).attr("href", shortUrl);
        });
        $("#tabs").tabs("load", $("#tabs").tabs('option', 'active'));
    }

    if (loadTab) { // The user pressed the "Get observations" button, so they're viewing a date range now.
        $("#set_or_date_range_label").html("date range");
        $("#set_details").hide();
    }

    $("#summary_table").html("<img src='/static/images/ajax-loader.gif' class='loading'/>");

    // Make each date into a string of the format "YYYY-mm-ddTHH:MM:SSZ", which is the format used in the local database.
    var startUTC = startDate.toISOString().slice(0, 19) + "Z";
    var endUTC = endDate.toISOString().slice(0, 19) + "Z";

    window.dataSummaryTableRequest = $.ajax({
        type: "POST",
        url: "/data_summary_table",
        data: {'starttime': startUTC, 'endtime': endUTC},
        success: function(data) {
            $("#summary_table").html(data);
        },
        dataType: "html"
    });
};

function getComments() {
    $("#comments_div").html("<img src='/static/images/ajax-loader.gif' class='loading'/>");

    $.ajax({
        type: "GET",
        url: "/get_all_comments",
        success: function(data) {
            $("#comments_div").html(data);
            $("#comments_list").collapsible({
                animate: false
            });
        },
        dataType: "html"
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

var applyFiltersAndSort = function() {
    var user = $("#user_setlist_dropdown").val();
    var eor = $("#eor_setlist_dropdown").val();
    var high_low = $("#high_low_setlist_dropdown").val();
    var sort = $("#sort_setlist_dropdown").val();
    var ranged = $("#range_filter").prop('checked');

    var set_controls = {
        'user': user,
        'eor': eor,
        'high_low': high_low,
        'sort': sort,
        'ranged': ranged
    };

    var start = $("#datepicker_start").val();
    var end = $("#datepicker_end").val();

    var startDate, endDate;

    startDate = getDate(start);
    endDate = getDate(end);

    // Make each date into a string of the format "YYYY-mm-ddTHH:MM:SSZ", which is the format used in the local database.
    var startUTC = startDate.toISOString().slice(0, 19) + "Z";
    var endUTC = endDate.toISOString().slice(0, 19) + "Z";

    renderSets(set_controls, startUTC, endUTC, false);
};
