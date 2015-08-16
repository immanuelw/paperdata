{% include "js/histogram_utils.js" %}

var _chart;
var inConstructionMode = false;
{% if is_set %}
var flaggedRanges = plot_bands;
var currentData = ['{{the_set.low_or_high}}', '{{the_set.eor}}'];
{% else %}
var lowEOR0FlaggedRanges = [], highEOR0FlaggedRanges = [];
var lowEOR1FlaggedRanges = [], highEOR1FlaggedRanges = [];
var flaggedRanges = highEOR0FlaggedRanges;
var currentData = ['high', 'EOR0'];
{% endif %}
var clickDragMode = 'zoom';

var setup = function() {
    $('#construction_controls').hide();
    {% if is_set %}
        // The flagged ranges don't have ids or labels yet.
        updateFlaggedRangeIdsAndLabels();
        // Draw the plot bands.
        addAllPlotBands();
        // Count the # of observations in each band.
        for (var i = 0; i < flaggedRanges.length; ++i) {
            var thisRange = flaggedRanges[i];
            var counts = getObsAndErrorCountInRange(thisRange.from, thisRange.to);
            thisRange.obs_count = counts.obsCount;
            thisRange.err_count = counts.errCount;
            thisRange.obs_start_index = counts.obsStartIndex;
            thisRange.obs_end_index = counts.obsEndIndex;
            thisRange.err_start_index = counts.errStartIndex;
            thisRange.err_end_index = counts.errEndIndex;
        }
        // Update the information in the panel.
        updateSetConstructionTable();
    {% endif %}
};

var saveSet = function() {
    var setSaveButton = function(text, disabled) {
        $('#save_set_button').html(text);
        $('#save_set_button').prop('disabled', disabled);
    };

    var currentObsIdMap = getCurrentObsIdMap();

    if (currentObsIdMap.length === 0) {
        alert("There aren't any obs ids in this set!");
        return;
    } else if ($('#set_name_textbox').val().length === 0) {
        alert("The set must have a name!");
        return;
    }

    setSaveButton("Working...", true);

    var getFlaggedObsIdsMapIndices = function(start_millis, end_millis) {
        var utc_obsid_map = getCurrentObsIdMap();

        var startIndex = 0, endIndex = 0;

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
            name: $('#set_name_textbox').val(),
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

var getCurrentDataSeries = function() {
    {% if is_set %} // The user can't change the EOR or low/high, so there is only one set of observations that
    return observation_counts; // corresponds to the EOR and low/high specified by the set.
    {% else %}
    if (currentData[0] === 'low' && currentData[1] === 'EOR0')
        return observation_counts_l0;
    else if (currentData[0] === 'low' && currentData[1] === 'EOR1')
        return observation_counts_l1;
    else if (currentData[0] === 'high' && currentData[1] === 'EOR0')
        return observation_counts_h0;
    else if (currentData[0] === 'high' && currentData[1] === 'EOR1')
        return observation_counts_h1;
    {% endif %}
};

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

var hideDataOnDataSetChange = function() {
    var suffix = getVariableSuffix();
    var remove = $("#remove_flagged_data_checkbox").is(":checked");
    var obsSeries = _chart.series[0];
    var errSeries = _chart.series[1];

    var obsSeriesData = copies['observation_counts' + suffix];
    var obsSeriesDataCopy = obsSeriesData.map(function(arr) {
        return arr.slice();
    });
    var errSeriesData = error_counts_copy;
    var errSeriesDataCopy = errSeriesData.map(function(arr) {
        return arr.slice();
    });

    if (remove) {
        for (var flaggedRangeIndex = 0; flaggedRangeIndex < flaggedRanges.length; ++flaggedRangeIndex) {
            var thisRange = flaggedRanges[flaggedRangeIndex];
            for (var obsIndex = thisRange.obs_start_index; obsIndex <= thisRange.obs_end_index; ++obsIndex) {
                obsSeriesDataCopy[obsIndex][1] = null;
            }
            for (var errIndex = thisRange.err_start_index; errIndex <= thisRange.err_end_index; ++errIndex) {
                errSeriesDataCopy[errIndex][1] = null;
            }
        }
    }

    obsSeries.setData(obsSeriesDataCopy, false);
    errSeries.setData(errSeriesDataCopy);
};

var dataSetChanged = function() {
    _chart.series[0].setData(getCurrentDataSeries());

    if (inConstructionMode) {
        removeAllPlotBands();

        // Set the correct flagged ranges.
        flaggedRanges = getCurrentFlaggedSet();
        hideDataOnDataSetChange();

        addAllPlotBands();

        // Update the information in the panel.
        updateSetConstructionTable();
    } else {
        // Set the correct flagged ranges.
        flaggedRanges = getCurrentFlaggedSet();
        hideDataOnDataSetChange();
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

var getDataIndices = function(startTime, endTime, obsSeries, errSeries) {
    var obsStartIndex = 0, obsEndIndex = -1;
    var errStartIndex = 0, errEndIndex = -1;

    for (var i = 0; i < obsSeries.xData.length; ++i) {
        if (obsSeries.xData[i] >= startTime) {
            obsStartIndex = i;
            obsEndIndex = obsSeries.xData.length - 1;
            break;
        }
    }

    for (var i = obsStartIndex; i < obsSeries.xData.length; ++i) {
        if (obsSeries.xData[i] > endTime) {
            obsEndIndex = i - 1;
            break;
        }
    }

    for (var i = 0; i < errSeries.xData.length; ++i) {
        if (errSeries.xData[i] >= startTime) {
            errStartIndex = i;
            errEndIndex = errSeries.xData.length - 1;
            break;
        }
    }

    for (var i = errStartIndex; i < errSeries.xData.length; ++i) {
        if (errSeries.xData[i] > endTime) {
            errEndIndex = i - 1;
            break;
        }
    }

    return {
        obsStartIndex: obsStartIndex,
        obsEndIndex: obsEndIndex,
        errStartIndex: errStartIndex,
        errEndIndex: errEndIndex
    };
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
            var counts = getObsAndErrorCountInRange(lowerRange.from, lowerRange.to);
            lowerRange.obs_count = counts.obsCount;
            lowerRange.err_count = counts.errCount;
            lowerRange.obs_start_index = counts.obsStartIndex;
            lowerRange.obs_end_index = counts.obsEndIndex;
            lowerRange.err_start_index = counts.errStartIndex;
            lowerRange.err_end_index = counts.errEndIndex;
        } else { // No overlap.
            flaggedRanges.push(higherRange);
        }
    }
};

var flagClickAndDraggedRange = function(event) {
    flagRangeInSet(event.xAxis[0].min, event.xAxis[0].max);
};

var getObsAndErrorCountInRange = function(startTime, endTime) {
    var obsSeries = _chart.series[0], errSeries = _chart.series[1];

    var dataIndices = getDataIndices(startTime, endTime, obsSeries, errSeries);

    var flaggedObs = getCurrentDataSeries().slice(dataIndices.obsStartIndex, dataIndices.obsEndIndex + 1);

    var flaggedErr = error_counts.slice(dataIndices.errStartIndex, dataIndices.errEndIndex + 1);

    var obsCount = 0, errCount = 0;

    for (var i = 0; i < flaggedObs.length; ++i) {
        obsCount += flaggedObs[i][1];
    }

    for (var i = 0; i < flaggedErr.length; ++i) {
        errCount += flaggedErr[i][1];
    }

    return {
        obsCount: obsCount,
        errCount: errCount,
        obsStartIndex: dataIndices.obsStartIndex,
        obsEndIndex: dataIndices.obsEndIndex,
        errStartIndex: dataIndices.errStartIndex,
        errEndIndex: dataIndices.errEndIndex
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
    var counts = getObsAndErrorCountInRange(startTime, endTime);

    var plotBand = {
        id: "",         // The id will be determined later.
        color: 'yellow',
        from: startTime,
        to: endTime,
        obs_count: counts.obsCount,
        err_count: counts.errCount,
        obs_start_index: counts.obsStartIndex,
        obs_end_index: counts.obsEndIndex,
        err_start_index: counts.errStartIndex,
        err_end_index: counts.errEndIndex
    };

    addedNewFlaggedRange(plotBand);

    updateSetConstructionTable();
};

var updateSetConstructionTable = function() {
    var tableHtml = "";

    for (var i = 0; i < flaggedRanges.length; ++i) {
        var flaggedRange = flaggedRanges[i];
        tableHtml += '<tr><td>' + flaggedRange.label.text + '</td>' +
        '<td>' + new Date(flaggedRange.from).toISOString() + '</td>' +
        '<td>' + new Date(flaggedRange.to).toISOString() + '</td>' +
        '<td>' + flaggedRange.obs_count + '</td>' +
        '<td>' + flaggedRange.err_count + '</td>' +
        '<td><button onclick=\'obs_err.unflagRange("' + flaggedRange.id +
        '")\'>Unflag range</button></td></tr>';
    }

    $('#set_construction_table > tbody').html(tableHtml);
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
    if ($("#remove_flagged_data_checkbox").is(":checked")) {
        var thisRange = flaggedRanges[index];
        var obsSeries = _chart.series[0];
        var errSeries = _chart.series[1];
        {% if is_set %}
        var obsSeriesData = observation_counts_copy;
        {% else %}
        var suffix = getVariableSuffix();
        var obsSeriesData = copies['observation_counts' + suffix];
        {% endif %}
        var errSeriesData = error_counts_copy;

        var obsSeriesGraphData = getSeriesDataCopyFromGraph(obsSeries);
        var errSeriesGraphData = getSeriesDataCopyFromGraph(errSeries);

        for (var obsIndex = thisRange.obs_start_index; obsIndex <= thisRange.obs_end_index; ++obsIndex) {
            obsSeriesGraphData[obsIndex][1] = obsSeriesData[obsIndex][1];
        }
        for (var errIndex = thisRange.err_start_index; errIndex <= thisRange.err_end_index; ++errIndex) {
            errSeriesGraphData[errIndex][1] = errSeriesData[errIndex][1];
        }

        obsSeries.setData(obsSeriesGraphData, false);
        errSeries.setData(errSeriesGraphData);
    }
};

var clickConstructionModeCheckbox = function(checkbox) {
    inConstructionMode = checkbox.checked;
    if (inConstructionMode) { // Entering construction mode.
        $('#construction_controls').show(); // Show set construction controls.
        clickDragMode = $("#click_drag_dropdown").val();
        {% if not is_set %} // If we're looking at a set, the plot bands will always be on the graph.
            addAllPlotBands(); // Also, because the user can't change the data set, the table doesn't need to be updated.

            // Update the information in the table.
            updateSetConstructionTable();
        {% endif %}
    } else { // Exiting construction mode.
        $('#construction_controls').hide(); // Hide set construction controls.
        clickDragMode = "zoom"; // Only allow zooming when not in construction mode.
        {% if not is_set %}
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
    if ($("#remove_flagged_data_checkbox").is(":checked")) {
        var obsSeries = _chart.series[0];
        var errSeries = _chart.series[1];
        var obsSeriesGraphData = getSeriesDataCopyFromGraph(obsSeries);
        var errSeriesGraphData = getSeriesDataCopyFromGraph(errSeries);

        for (var obsIndex = flaggedRange.obs_start_index; obsIndex <= flaggedRange.obs_end_index; ++obsIndex) {
            obsSeriesGraphData[obsIndex][1] = null;
        }
        for (var errIndex = flaggedRange.err_start_index; errIndex <= flaggedRange.err_end_index; ++errIndex) {
            errSeriesGraphData[errIndex][1] = null;
        }

        obsSeries.setData(obsSeriesGraphData, false);
        errSeries.setData(errSeriesGraphData);
    }
};

var clickRemoveFlaggedDataCheckbox = function(checkbox) {
    var remove = checkbox.checked;

    var obsSeries = _chart.series[0];
    var errSeries = _chart.series[1];

    if (!remove) {
        {% if is_set %}
        var obsSeriesData = observation_counts_copy;
        {% else %}
        var suffix = getVariableSuffix();
        var obsSeriesData = copies['observation_counts' + suffix];
        {% endif %}
        var errSeriesData = error_counts_copy;
    }
    var obsSeriesGraphData = getSeriesDataCopyFromGraph(obsSeries);
    var errSeriesGraphData = getSeriesDataCopyFromGraph(errSeries);

    for (var flaggedRangeIndex = 0; flaggedRangeIndex < flaggedRanges.length; ++flaggedRangeIndex) {
        var thisRange = flaggedRanges[flaggedRangeIndex];
        for (var obsIndex = thisRange.obs_start_index; obsIndex <= thisRange.obs_end_index; ++obsIndex) {
            if (remove) {
                obsSeriesGraphData[obsIndex][1] = null;
            } else {
                obsSeriesGraphData[obsIndex][1] = obsSeriesData[obsIndex][1];
            }
        }
        for (var errIndex = thisRange.err_start_index; errIndex <= thisRange.err_end_index; ++errIndex) {
            if (remove) {
                errSeriesGraphData[errIndex][1] = null;
            } else {
                errSeriesGraphData[errIndex][1] = errSeriesData[errIndex][1];
            }
        }
    }

    obsSeries.setData(obsSeriesGraphData, false);
    errSeries.setData(errSeriesGraphData);
};
dataSourceObj.clickRemoveFlaggedDataCheckbox = clickRemoveFlaggedDataCheckbox;

var sliderChanged = function(slider) {
    var newOptions = {
        dataGrouping: {
            groupPixelWidth: slider.value
        }
    };

    _chart.series[0].update(newOptions, false); // Don't redraw chart.
    _chart.series[1].update(newOptions);
};
dataSourceObj.sliderChanged = sliderChanged;
