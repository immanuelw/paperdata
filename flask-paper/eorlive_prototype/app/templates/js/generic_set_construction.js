var _chart;
var inConstructionMode = false;
{% if is_set %}
var flaggedRanges = {{ plot_bands | tojson }};
var currentData = ['{{the_set.low_or_high}}', '{{the_set.eor}}'];
{% else %}
var lowEOR0FlaggedRanges = [], highEOR0FlaggedRanges = [];
var lowEOR1FlaggedRanges = [], highEOR1FlaggedRanges = [];
var flaggedRanges = highEOR0FlaggedRanges;
var currentData = ['high', 'EOR0'];
{% endif %}
var clickDragMode = 'zoom';
var dataSourceObj = {};

var setup = function() {
    $('#construction_controls_{{data_source_str_nospace}}').hide();
    {% if is_set %}
        // The flagged ranges don't have ids or labels yet.
        updateFlaggedRangeIdsAndLabels();
        // Draw the plot bands.
        addAllPlotBands();
        // Count the # of observations in each band.
        for (var i = 0; i < flaggedRanges.length; ++i) {
            var thisRange = flaggedRanges[i];
            var obsCountForRange = getObsCountInRange(thisRange.from, thisRange.to);
            thisRange.obs_count = obsCountForRange.obsCount;
            thisRange.start_index = obsCountForRange.startIndex;
            thisRange.end_index = obsCountForRange.endIndex;
        }
        // Update the information in the panel.
        updateSetConstructionTable();
    {% endif %}
};

var saveSet = function() {
    var setSaveButton = function(text, disabled) {
        $('#save_set_button_{{data_source_str_nospace}}').html(text);
        $('#save_set_button_{{data_source_str_nospace}}').prop('disabled', disabled);
    };

    var currentObsIdMap = getCurrentObsIdMap();

    if (currentObsIdMap.length === 0) {
        alert("There aren't any obs ids in this set!");
        return;
    } else if ($('#set_name_textbox_{{data_source_str_nospace}}').val().length === 0) {
        alert("The set must have a name!");
        return;
    }

    setSaveButton("Working...", true);

    var getFlaggedObsIdsMapIndices = function(start_millis, end_millis) {
        var utc_obsid_map = getCurrentObsIdMap();

        var startIndex = 0; endIndex = 0;

        for (var i = 0; i < utc_obsid_map.length; ++i) {
            if (utc_obsid_map[i][0] >= start_millis) {
                startIndex = i;
                break;
            }
        }

        for (var i = startIndex; i < utc_obsid_map.length; ++i) {
            if (utc_obsid_map[i][0] > end_millis) {
                endIndex = i - 1;
                break;
            } else if (i === utc_obsid_map.length - 1) {
                endIndex = i;
                break;
            }
        }

        return {
            startObsIdMapIndex: startIndex,
            endObsIdMapIndex: endIndex
        };
    };

    var rangesOfFlaggedObsIds = [];

    for (var i = 0; i < flaggedRanges.length; ++i) {
        if (flaggedRanges[i].obs_count > 0) { // There are observations in this range!
            var indices = getFlaggedObsIdsMapIndices(flaggedRanges[i].from, flaggedRanges[i].to);
            var rangeOfFlaggedObsIds = currentObsIdMap.slice(indices.startObsIdMapIndex, indices.endObsIdMapIndex + 1);
            rangesOfFlaggedObsIds.push({
                start_millis: flaggedRanges[i].from,
                end_millis: flaggedRanges[i].to,
                flaggedRange: rangeOfFlaggedObsIds
            });
        }
    }

    $.ajax({
        type: "POST",
        url: "/save_new_set",
        data: JSON.stringify({
            name: $('#set_name_textbox_{{data_source_str_nospace}}').val(),
            startObsId: currentObsIdMap[0][1],
            endObsId: currentObsIdMap[currentObsIdMap.length - 1][1],
            flaggedRanges: rangesOfFlaggedObsIds,
            lowOrHigh: currentData[0],
            eor: currentData[1]
        }),
        success: function(data) {
            if (data.error) {
                alert(data.message);
            } else {
                alert("Set saved successfully!");
                //refresh the set view
                applyFiltersAndSort();

                //update the view status
                var setName = $('#set_name_textbox_{{data_source_str_nospace}}').val();
                $('#view_status_span').html("You are viewing a set (" + setName + ")");
            }

            setSaveButton("Save set", false);
        },
        error: function(xhr, status, error) {
            alert("An error occured: " + status);
            setSaveButton("Save set", false);
        },
        contentType: 'application/json',
        dataType: 'json'
    });
};
dataSourceObj.saveSet = saveSet;

var getCurrentFlaggedSet = function() {
    if (currentData[0] === 'low' && currentData[1] === 'EOR0')
        return lowEOR0FlaggedRanges;
    else if (currentData[0] === 'low' && currentData[1] === 'EOR1')
        return lowEOR1FlaggedRanges;
    else if (currentData[0] === 'high' && currentData[1] === 'EOR0')
        return highEOR0FlaggedRanges;
    else if (currentData[0] === 'high' && currentData[1] === 'EOR1')
        return highEOR1FlaggedRanges;
};

var getCurrentObsIdMap = function() {
    if (currentData[0] === 'low' && currentData[1] === 'EOR0')
        return utc_obsid_map_l0;
    else if (currentData[0] === 'low' && currentData[1] === 'EOR1')
        return utc_obsid_map_l1;
    else if (currentData[0] === 'high' && currentData[1] === 'EOR0')
        return utc_obsid_map_h0;
    else if (currentData[0] === 'high' && currentData[1] === 'EOR1')
        return utc_obsid_map_h1;
    else
        return utc_obsid_map_any;
};

var getVariableSuffix = function() {
    if (currentData[0] === 'low' && currentData[1] === 'EOR0') {
        return '_l0';
    } else if (currentData[0] === 'low' && currentData[1] === 'EOR1') {
        return '_l1';
    } else if (currentData[0] === 'high' && currentData[1] === 'EOR0') {
        return '_h0';
    } else if (currentData[0] === 'high' && currentData[1] === 'EOR1') {
        return '_h1';
    }
};

var updateAllDataSeriesWithHiddenData = function() {
    var suffix = getVariableSuffix();
    var remove = $("#remove_flagged_data_checkbox_{{data_source_str_nospace}}").is(":checked");
    for (var seriesIndex = 0; seriesIndex < _chart.series.length - 1; ++seriesIndex) {
        var thisSeries = _chart.series[seriesIndex];

        var seriesData = graph_data[thisSeries.name + suffix + "_copy"];
        var seriesDataCopy = seriesData.map(function(arr) {
            return arr.slice();
        });

        if (remove) {
            for (var flaggedRangeIndex = 0; flaggedRangeIndex < flaggedRanges.length; ++flaggedRangeIndex) {
                var thisRange = flaggedRanges[flaggedRangeIndex];
                for (var dataIndex = thisRange.start_index; dataIndex <= thisRange.end_index; ++dataIndex) {
                    seriesDataCopy[dataIndex][1] = null;
                }
            }
        }

        if (seriesIndex < _chart.series.length - 2) {
            thisSeries.setData(seriesDataCopy, false); // Don't redraw chart.
        } else {
            thisSeries.setData(seriesDataCopy); // Redraw chart.
        }
    }
};

var dataSetChanged = function() {
    if (inConstructionMode) {
        removeAllPlotBands();

        // Set the correct flagged ranges.
        flaggedRanges = getCurrentFlaggedSet();

        updateAllDataSeriesWithHiddenData();

        addAllPlotBands();

        // Update the information in the panel.
        updateSetConstructionTable();
    } else {
        // Set the correct flagged ranges.
        flaggedRanges = getCurrentFlaggedSet();

        updateAllDataSeriesWithHiddenData();
    }
};

var setEorData = function(select) {
    currentData[1] = select.value;

    dataSetChanged();
};
dataSourceObj.setEorData = setEorData;

var setLowOrHighData = function(select) {
    currentData[0] = select.value;

    dataSetChanged();
};
dataSourceObj.setLowOrHighData = setLowOrHighData;

var setClickDragMode = function(select) {
    clickDragMode = select.value;
};
dataSourceObj.setClickDragMode = setClickDragMode;

var clearSetConstructionData = function() {
    flaggedRanges = [];
    lowEOR0FlaggedRanges = [];
    highEOR0FlaggedRanges = [];
    lowEOR1FlaggedRanges = [];
    highEOR1FlaggedRanges = [];
};

var mergeOverlappingRanges = function() {
    if (flaggedRanges.length === 0)
        return;

    var comparator = function(a, b) {
        if (a.from < b.from)
            return -1;
        else if (a.from > b.from)
            return 1;
        return 0;
    };

    // Need to copy by slicing because sort() returns the reference
    // to the array tracking flagged ranges, and we need to empty
    // that array.
    var sortedFlaggedRanges = flaggedRanges.sort(comparator).slice();

    flaggedRanges.length = 0; // Maintain reference to flaggedRanges,
                              // which points to the correct underlying
                              // subset.

    flaggedRanges.push(sortedFlaggedRanges[0]);

    for (var i = 1; i < sortedFlaggedRanges.length; ++i) {
        var lowerRange = flaggedRanges[flaggedRanges.length - 1]; // Get top element in stack.
        var higherRange = sortedFlaggedRanges[i];

        if (higherRange.from <= lowerRange.to) { // Current interval overlaps with previous interval.
            lowerRange.to = Math.max(lowerRange.to, higherRange.to);

            // Since we merged two intervals, we have to update the observation & error counts.
            var obsCountForRange = getObsCountInRange(lowerRange.from, lowerRange.to);
            lowerRange.obs_count = obsCountForRange.obsCount;
            lowerRange.start_index = obsCountForRange.startIndex;
            lowerRange.end_index = obsCountForRange.endIndex;
        } else { // No overlap.
            flaggedRanges.push(higherRange);
        }
    }
};

var flagClickAndDraggedRange = function(event) {
    flagRangeInSet(event.xAxis[0].min, event.xAxis[0].max);
};

var getObsCountInRange = function(startTime, endTime) {
    var currentObsIdMap = getCurrentObsIdMap();
    var startIndex = 0, endIndex = 0;

    for (var i = 0; i < currentObsIdMap.length; ++i) {
        if (currentObsIdMap[i][0] >= startTime) {
            startIndex = i;
            break;
        }
    }

    for (var i = 0; i < currentObsIdMap.length; ++i) {
        if (currentObsIdMap[i][0] > endTime) {
            endIndex = i - 1;
            break;
        } else if (i === currentObsIdMap.length - 1) { // At end of list but haven't found range end yet.
            endIndex = i;
            break;
        }
    }

    var obsCount = endIndex - startIndex + 1;

    return {
        obsCount: obsCount,
        startIndex: startIndex,
        endIndex: endIndex
    };
};

var updateFlaggedRangeIdsAndLabels = function() {
    for (var i = 0; i < flaggedRanges.length; ++i) {
        flaggedRanges[i].id = (currentData[0] + currentData[1] + i).toString();
        flaggedRanges[i].label = { text: (i + 1).toString() };
    }
};

var addAllPlotBands = function() {
    for (var i = 0; i < flaggedRanges.length; ++i) {
        _chart.xAxis[0].addPlotBand(flaggedRanges[i]);
    }
};

var removeAllPlotBands = function() {
    for (var i = 0; i < flaggedRanges.length; ++i) {
        _chart.xAxis[0].removePlotBand(flaggedRanges[i].id);
    }
};

var addedNewFlaggedRange = function(plotBand) {
    removeAllPlotBands();

    // Add new plot band.
    flaggedRanges.push(plotBand);
    removeDataForNewFlaggedRangeIfNecessary(plotBand);
    mergeOverlappingRanges();
    updateFlaggedRangeIdsAndLabels();

    addAllPlotBands();
};

var flagRangeInSet = function(startTime, endTime) {
    var obs_count_in_range = getObsCountInRange(startTime, endTime);

    var plotBand = {
        id: "",         // The id will be determined later.
        color: 'yellow',
        from: startTime,
        to: endTime,
        obs_count: obs_count_in_range.obsCount,
        start_index: obs_count_in_range.startIndex,
        end_index: obs_count_in_range.endIndex
    };

    addedNewFlaggedRange(plotBand);

    updateSetConstructionTable();
};

var updateSetConstructionTable = function() {
    var tableHtml = "";

    for (var i = 0; i < flaggedRanges.length; ++i) {
        var flaggedRange = flaggedRanges[i];
        var checkedStr = flaggedRanges[i].dataRemoved ? "checked" : "";
        tableHtml += '<tr><td>' + flaggedRange.label.text + '</td>' +
        '<td>' + new Date(flaggedRange.from).toISOString() + '</td>' +
        '<td>' + new Date(flaggedRange.to).toISOString() + '</td>' +
        '<td>' + flaggedRange.obs_count + '</td>' +
        '<td><button onclick=\'{{data_source_str_nospace}}.unflagRange("' +
        flaggedRange.id + '")\'>Unflag range</button></td></tr>';
    }

    $('#set_construction_table_{{data_source_str_nospace}} > tbody').html(tableHtml);
};

var removedFlaggedRange = function(index) {
    removeAllPlotBands();
    reinsertDataForRange(index);
    flaggedRanges.splice(index, 1);
    updateFlaggedRangeIdsAndLabels();
    addAllPlotBands();
};

var unflagRange = function(flaggedRangeId) {
    for (var i = 0; i < flaggedRanges.length; ++i) {
        if (flaggedRanges[i].id === flaggedRangeId) {
            removedFlaggedRange(i);
            break;
        }
    }

    updateSetConstructionTable();
};
dataSourceObj.unflagRange = unflagRange;

var reinsertDataForRange = function(index) {
    if ($("#remove_flagged_data_checkbox_{{data_source_str_nospace}}").is(":checked")) {
        var thisRange = flaggedRanges[index];
        var suffix = getVariableSuffix();
        for (var seriesIndex = 0; seriesIndex < _chart.series.length - 1; ++seriesIndex) {
            var thisSeries = _chart.series[seriesIndex];

            {% if is_set %}
                var seriesData = copies[thisSeries.name];
            {% else %}
                var seriesData = graph_data[thisSeries.name + suffix + "_copy"];
            {% endif %}
            var seriesDataCopy = getSeriesDataCopyFromGraph(thisSeries);

            for (var dataIndex = thisRange.start_index; dataIndex <= thisRange.end_index; ++dataIndex) {
                seriesDataCopy[dataIndex][1] = seriesData[dataIndex][1];
            }

            if (seriesIndex < _chart.series.length - 2) {
                thisSeries.setData(seriesDataCopy, false);
            } else {
                thisSeries.setData(seriesDataCopy);
            }
        }
    }
};

var clickConstructionModeCheckbox = function(checkbox) {
    inConstructionMode = checkbox.checked;
    if (inConstructionMode) { // Entering construction mode.
        $('#construction_controls_{{data_source_str_nospace}}').show(); // Show set construction controls.
        clickDragMode = $("#click_drag_dropdown_{{data_source_str_nospace}}").val();
        {% if not is_set %} // If we're looking at a set, the plot bands will always be on the graph.
            addAllPlotBands(); // Also, because the user can't change the data set, the table doesn't need to be updated.

            // Update the information in the table.
            updateSetConstructionTable();
        {% endif %}
    } else { // Exiting construction mode.
        $('#construction_controls_{{data_source_str_nospace}}').hide(); // Hide set construction controls.
        clickDragMode = "zoom"; // Only allow zooming when not in construction mode.
        {% if not is_set %} // If we're looking at a set, the plot bands will always be on the graph.
            removeAllPlotBands();
        {% endif %}
    }
};
dataSourceObj.clickConstructionModeCheckbox = clickConstructionModeCheckbox;

var getSeriesDataCopyFromGraph = function(series) {
    var arrayCopy = [];
    for (var i = 0; i < series.xData.length; ++i) {
        arrayCopy[i] = [series.xData[i], series.yData[i]];
    }
    return arrayCopy;
};

var removeDataForNewFlaggedRangeIfNecessary = function(flaggedRange) {
    if ($("#remove_flagged_data_checkbox_{{data_source_str_nospace}}").is(":checked")) {
        for (var seriesIndex = 0; seriesIndex < _chart.series.length - 1; ++seriesIndex) {
            var thisSeries = _chart.series[seriesIndex];
            var seriesDataCopy = getSeriesDataCopyFromGraph(thisSeries);

            for (var dataIndex = flaggedRange.start_index; dataIndex <= flaggedRange.end_index; ++dataIndex) {
                seriesDataCopy[dataIndex][1] = null;
            }

            if (seriesIndex < _chart.series.length - 2) {
                thisSeries.setData(seriesDataCopy, false); // Don't redraw chart.
            } else {
                thisSeries.setData(seriesDataCopy); // Redraw chart.
            }
        }
    }
};

var clickRemoveFlaggedDataCheckbox = function(checkbox) {
    var remove = checkbox.checked;

    for (var seriesIndex = 0; seriesIndex < _chart.series.length - 1; ++seriesIndex) {
        var thisSeries = _chart.series[seriesIndex];
        if (!remove) {
            {% if is_set %}
            var seriesData = copies[thisSeries.name]; // Get the original, unmodified copy of the data since
            {% else %}                                // Highcharts modifies the data used to create the chart.
            var suffix = getVariableSuffix();
            var seriesData = graph_data[thisSeries.name + suffix + "_copy"];
            {% endif %}
        }
        var seriesDataCopy = getSeriesDataCopyFromGraph(thisSeries);

        for (var flaggedRangeIndex = 0; flaggedRangeIndex < flaggedRanges.length; ++flaggedRangeIndex) {
            var thisRange = flaggedRanges[flaggedRangeIndex];
            for (var dataIndex = thisRange.start_index; dataIndex <= thisRange.end_index; ++dataIndex) {
                if (remove) {
                    seriesDataCopy[dataIndex][1] = null;
                } else {
                    seriesDataCopy[dataIndex][1] = seriesData[dataIndex][1];
                }
            }
        }

        if (seriesIndex < _chart.series.length - 2) {
            thisSeries.setData(seriesDataCopy, false); // Don't redraw chart.
        } else {
            thisSeries.setData(seriesDataCopy); // Redraw chart.
        }
    }
};
dataSourceObj.clickRemoveFlaggedDataCheckbox = clickRemoveFlaggedDataCheckbox;

{% if width_slider %}
    {% include 'js/width_slider.js' %}
    dataSourceObj.sliderChanged = sliderChanged;
{% endif %}
