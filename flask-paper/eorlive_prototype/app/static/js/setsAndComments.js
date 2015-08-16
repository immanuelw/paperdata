function renderSets(set_controls, startUTC, endUTC, includeDeleteButtons) {
    $("#set_list_div").html("<img src='/static/images/ajax-loader.gif' class='loading'/>");

    window.setRequest = $.ajax({
        type: "POST",
        url: "/get_sets",
        data: JSON.stringify({
            'set_controls': set_controls,
            'starttime': startUTC,
            'endtime': endUTC,
            'includeDeleteButtons': includeDeleteButtons
        }),
        success: function(data) {
            $("#set_list_div").html(data);
        },
        contentType: 'application/json',
        dataType: 'html'
    });
};

function deleteSet(setName) {
    $.ajax({
        type: "POST",
        url: "/delete_set",
        data: {'set_name': setName},
        success: function(data) {
            document.write(data);
        },
        dataType: 'html'
    });
};
