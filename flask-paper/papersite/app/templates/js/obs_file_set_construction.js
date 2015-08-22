{% include 'js/histogram_utils.js' %}

var _chart;
var inConstructionMode = false;
var	pol_strs = ['all', 'xx', 'xy', 'yx', 'yy']
var	era_type_strs = ['all',]
var	host_strs = ['all', 'pot1', 'pot2', 'pot3', 'folio', 'pot8', 'nas1']
var	filetype_strs = ['all', 'uv', 'uvcRRE', 'npz']
{% if is_set %}
var flaggedObsRanges = plot_bands;
var currentObsData = {'polarization': '{{ the_set.polarization }}', 'era_type': '{{ the_set.era_type }}'};
var currentFileData = {'host': '{{ the_set.host }}', 'filetype': '{{ the_set.filetype }}'};
{% else %}
var flaggedObsDict = {pol_str: {era_type_str: [] for (era_type_str of era_type_strs)} for (pol_str of pol_strs)};
var flaggedObsRanges = flaggedObsDict['all']['all'];
var currentObsData = {'polarization': 'all', 'era_type': 'all'};
var flaggedFileDict = {host_str: {filetype_str: [] for (filetype_str of filetype_strs)} for (host_str of host_strs)};
var flaggedFileRanges = flaggedFileDict['all']['all'];
var currentFileData = {'host': 'all', 'filetype': 'all'};
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
		for (var i = 0; i < flaggedObsRanges.length; ++i) {
			var thisRange = flaggedObsRanges[i];
			var counts = getObsAndFileCountInRange(thisRange.from, thisRange.to);
			thisRange.obs_count = counts.obsCount;
			thisRange.file_count = counts.fileCount;
			thisRange.obs_start_index = counts.obsStartIndex;
			thisRange.obs_end_index = counts.obsEndIndex;
			thisRange.file_start_index = counts.fileStartIndex;
			thisRange.file_end_index = counts.fileEndIndex;
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
	var currentFileIdMap = getCurrentFileIdMap();

	if (currentObsIdMap.length === 0) {
		alert('There aren't any obs ids in this set!');
		return;
	} else if ($('#set_name_textbox').val().length === 0) {
		alert('The set must have a name!');
		return;
	}

	setSaveButton('Working...', true);

	var getFlaggedObsIdsMapIndices = function(start_millis, end_millis) {
		var obs_map = getCurrentObsIdMap();

		var startIndex = 0, endIndex = 0;

		for (var i = 0; i < obs_map.length; ++i) {
			if (obs_map[i]['obs_time'] >= start_millis) {
				startIndex = i;
				break;
			}
		}

		for (var i = startIndex; i < obs_map.length; ++i) {
			if (obs_map[i]['obs_time'] > end_millis) {
				endIndex = i - 1;
				break;
			} else if (i === obs_map.length - 1) {
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

	for (var i = 0; i < flaggedObsRanges.length; ++i) {
		if (flaggedObsRanges[i].obs_count > 0) { // There are observations in this range!
			var indices = getFlaggedObsIdsMapIndices(flaggedObsRanges[i].from, flaggedObsRanges[i].to);
			var rangeOfFlaggedObsIds = currentObsIdMap.slice(indices.startObsIdMapIndex, indices.endObsIdMapIndex + 1);
			rangesOfFlaggedObsIds.push({
				start_millis: flaggedObsRanges[i].from,
				end_millis: flaggedObsRanges[i].to,
				flaggedRange: rangeOfFlaggedObsIds
			});
		}
	}

	$.ajax({
		type: 'POST',
		url: '/save_new_set',
		data: JSON.stringify({
			name: $('#set_name_textbox').val(),
			startObsId: currentObsIdMap[0]['obsnum'],
			endObsId: currentObsIdMap[currentObsIdMap.length - 1]['obsnum'],
			flaggedObsRanges: rangesOfFlaggedObsIds,
			polarization: currentObsData['polarization'],
			era_type: currentObsData['era_type'],
			host: currentFileData['host'],
			filetype: currentFileData['filetype']
		}),
		success: function(data) {
			if (data.error) {
				alert(data.message);
			} else {
				alert('Set saved successfully!');
				//refresh the set view
				applyFiltersAndSort();
			}

			setSaveButton('Save set', false);
		},
		error: function(xhr, status, error) {
			alert('An error occured: ' + status);
			setSaveButton('Save set', false);
		},
		contentType: 'application/json',
		dataType: 'json'
	});
};
dataSourceObj.saveSet = saveSet;

var getCurrentObsDataSeries = function() {
	{% if is_set %} // The user can't change the EOR or low/high, so there is only one set of observations that
	return observation_counts; // corresponds to the EOR and low/high specified by the set.
	{% else %}
	var polarization = currentObsData['polarization'];
	var era_type = currentObsData['era_type'];
	return obs_counts[polarization][era_type];
	{% endif %}
};

var getCurrentFlaggedSet = function() {
	var polarization = currentObsData['polarization'];
	var era_type = currentObsData['era_type'];
	return flaggedObsDict[polarization][era_type];
};

var getCurrentObsIdMap = function() {
	var polarization = currentObsData['polarization'];
	var era_type = currentObsData['era_type'];
	return obs_map[polarization][era_type];
};

var getCurrentFileDataSeries = function() {
	{% if is_set %} // The user can't change the EOR or low/high, so there is only one set of fileervations that
	return file_counts; // corresponds to the EOR and low/high specified by the set.
	{% else %}
	var host = currentFileData['host'];
	var filetype = currentFileData['filetype'];
	return file_counts[host][filetype];
	{% endif %}
};

var getCurrentFlaggedSet = function() {
	var host = currentFileData['host'];
	var filetype = currentFileData['filetype'];
	return flaggedFileDict[host][filetype];
};

var getCurrentFileIdMap = function() {
	var host = currentFileData['host'];
	var filetype = currentFileData['filetype'];
	return file_map[host][filetype];
};

var hideDataOnDataSetChange = function() {
	var remove = $('#remove_flagged_data_checkbox').is(':checked');
	var obsSeries = _chart.series[0];
	var fileSeries = _chart.series[1];

	var polarization = currentObsData['polarization'];
	var era_type = currentObsData['era_type'];
	var obsSeriesData = obs_counts[polarization][era_type];
	var obsSeriesDataCopy = obsSeriesData.map(function(arr) {
		return arr.slice();
	});
	var host = currentFileData['host'];
	var filetype = currentFileData['filetype'];
	var fileSeriesData = file_counts[host][filetype];
	var fileSeriesDataCopy = fileSeriesData.map(function(arr) {
		return arr.slice();
	});

	if (remove) {
		for (var flaggedRangeIndex = 0; flaggedRangeIndex < flaggedObsRanges.length; ++flaggedRangeIndex) {
			var thisRange = flaggedObsRanges[flaggedRangeIndex];
			for (var obsIndex = thisRange.obs_start_index; obsIndex <= thisRange.obs_end_index; ++obsIndex) {
				obsSeriesDataCopy[obsIndex]['obsnum'] = null;
			}
			for (var fileIndex = thisRange.file_start_index; fileIndex <= thisRange.file_end_index; ++fileIndex) {
				fileSeriesDataCopy[fileIndex]['obsnum'] = null;
			}
		}
	}

	obsSeries.setData(obsSeriesDataCopy, false);
	fileSeries.setData(fileSeriesDataCopy);
};

var dataSetChanged = function() {
	_chart.series[0].setData(getCurrentObsDataSeries());
	_chart.series[1].setData(getCurrentFileDataSeries());

	if (inConstructionMode) {
		removeAllPlotBands();

		// Set the correct flagged ranges.
		flaggedObsRanges = getCurrentFlaggedSet();
		flaggedFileRanges = getCurrentFlaggedSet();
		hideDataOnDataSetChange();

		addAllPlotBands();

		// Update the information in the panel.
		updateSetConstructionTable();
	} else {
		// Set the correct flagged ranges.
		flaggedObsRanges = getCurrentFlaggedSet();
		flaggedFileRanges = getCurrentFlaggedSet();
		hideDataOnDataSetChange();
	}
};

var setPolarizationData = function(select) {
	currentObsData['polarization'] = select.value;

	dataSetChanged();
};
dataSourceObj.setPolarizationData = setPolarizationData;

var setEraTypeData = function(select) {
	currentObsData['era_type'] = select.value;

	dataSetChanged();
};
dataSourceObj.setEraTypeData = setEraTypeData;

var setHostData = function(select) {
	currentFileData['host'] = select.value;

	dataSetChanged();
};
dataSourceObj.setHostData = setHostData;

var setFiletypeData = function(select) {
	currentFileData['filetype'] = select.value;

	dataSetChanged();
};
dataSourceObj.setFiletypeData = setFiletypeData;

var setClickDragMode = function(select) {
	clickDragMode = select.value;
};
dataSourceObj.setClickDragMode = setClickDragMode;

var clearSetConstructionData = function() {
	flaggedObsRanges = [];
	flaggedObsDict = {pol_str: {era_type_str: [] for (era_type_str of era_type_strs} for (pol_str of pol_strs)};
	flaggedFileRanges = [];
	flaggedFileDict = {host_str: {filetype_str: [] for (filetype_str of filetype_strs)} for (host_str of host_strs)};
};

var getDataIndices = function(startTime, endTime, obsSeries, fileSeries) {
	var obsStartIndex = 0, obsEndIndex = -1;
	var fileStartIndex = 0, fileEndIndex = -1;

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

	for (var i = 0; i < fileSeries.xData.length; ++i) {
		if (fileSeries.xData[i] >= startTime) {
			fileStartIndex = i;
			fileEndIndex = fileSeries.xData.length - 1;
			break;
		}
	}

	for (var i = fileStartIndex; i < fileSeries.xData.length; ++i) {
		if (fileSeries.xData[i] > endTime) {
			fileEndIndex = i - 1;
			break;
		}
	}

	return {
		obsStartIndex: obsStartIndex,
		obsEndIndex: obsEndIndex,
		fileStartIndex: fileStartIndex,
		fileEndIndex: fileEndIndex
	};
};

var mergeOverlappingRanges = function() {
	if (flaggedObsRanges.length === 0)
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
	var sortedObsFlaggedRanges = flaggedObsRanges.sort(comparator).slice();

	flaggedObsRanges.length = 0; // Maintain reference to flaggedObsRanges,
							  // which points to the correct underlying
							  // subset.

	flaggedObsRanges.push(sortedFlaggedRanges[0]);

	for (var i = 1; i < sortedFlaggedRanges.length; ++i) {
		var lowerRange = flaggedObsRanges[flaggedObsRanges.length - 1]; // Get top element in stack.
		var higherRange = sortedFlaggedRanges[i];

		if (higherRange.from <= lowerRange.to) { // Current interval overlaps with previous interval.
			lowerRange.to = Math.max(lowerRange.to, higherRange.to);

			// Since we merged two intervals, we have to update the observation & error counts.
			var counts = getObsAndFileCountInRange(lowerRange.from, lowerRange.to);
			lowerRange.obs_count = counts.obsCount;
			lowerRange.file_count = counts.fileCount;
			lowerRange.obs_start_index = counts.obsStartIndex;
			lowerRange.obs_end_index = counts.obsEndIndex;
			lowerRange.file_start_index = counts.fileStartIndex;
			lowerRange.file_end_index = counts.fileEndIndex;
		} else { // No overlap.
			flaggedObsRanges.push(higherRange);
		}
	}
};

var flagClickAndDraggedRange = function(event) {
	flagRangeInSet(event.xAxis[0].min, event.xAxis[0].max);
};

var getObsAndFileCountInRange = function(startTime, endTime) {
	var obsSeries = _chart.series[0], fileSeries = _chart.series[1];

	var dataIndices = getDataIndices(startTime, endTime, obsSeries, fileSeries);

	var flaggedObs = getCurrentObsDataSeries().slice(dataIndices.obsStartIndex, dataIndices.obsEndIndex + 1);

	var flaggedFile = file_counts.slice(dataIndices.fileStartIndex, dataIndices.fileEndIndex + 1);

	var obsCount = 0, fileCount = 0;

	for (var i = 0; i < flaggedObs.length; ++i) {
		obsCount += flaggedObs[i][1];
	}

	for (var i = 0; i < flaggedFile.length; ++i) {
		fileCount += flaggedFile[i][1];
	}

	return {
		obsCount: obsCount,
		fileCount: fileCount,
		obsStartIndex: dataIndices.obsStartIndex,
		obsEndIndex: dataIndices.obsEndIndex,
		fileStartIndex: dataIndices.fileStartIndex,
		fileEndIndex: dataIndices.fileEndIndex
	};
};

var updateFlaggedRangeIdsAndLabels = function() {
	for (var i = 0; i < flaggedObsRanges.length; ++i) {
		flaggedObsRanges[i].id = (currentObsData['polarization'] + currentData['era_type'] + i).toString();
		flaggedObsRanges[i].label = { text: (i + 1).toString() };
	}
	for (var i = 0; i < flaggedFileRanges.length; ++i) {
		flaggedFileRanges[i].id = (currentFileData['host'] + currentData['filetype'] + i).toString();
		flaggedFileRanges[i].label = { text: (i + 1).toString() };
	}
};

var addAllPlotBands = function() {
	for (var i = 0; i < flaggedObsRanges.length; ++i) {
		_chart.xAxis[0].addPlotBand(flaggedObsRanges[i]);
	}
	for (var i = 0; i < flaggedFileRanges.length; ++i) {
		_chart.xAxis[1].addPlotBand(flaggedFileRanges[i]);
	}
};

var removeAllPlotBands = function() {
	for (var i = 0; i < flaggedObsRanges.length; ++i) {
		_chart.xAxis[0].removePlotBand(flaggedObsRanges[i].id);
	}
	for (var i = 0; i < flaggedFileRanges.length; ++i) {
		_chart.xAxis[1].removePlotBand(flaggedFileRanges[i].id);
	}
};

var addedNewFlaggedRange = function(plotBand) {
	removeAllPlotBands();

	// Add new plot band.
	flaggedObsRanges.push(plotBand);
	flaggedFileRanges.push(plotBand);
	removeDataForNewFlaggedRangeIfNecessary(plotBand);
	mergeOverlappingRanges();
	updateFlaggedRangeIdsAndLabels();

	addAllPlotBands();
};

var flagRangeInSet = function(startTime, endTime) {
	var counts = getObsAndErrorCountInRange(startTime, endTime);

	var plotBand = {
		id: '',		 // The id will be determined later.
		color: 'yellow',
		from: startTime,
		to: endTime,
		obs_count: counts.obsCount,
		file_count: counts.fileCount,
		obs_start_index: counts.obsStartIndex,
		obs_end_index: counts.obsEndIndex,
		file_start_index: counts.fileStartIndex,
		file_end_index: counts.fileEndIndex
	};

	addedNewFlaggedRange(plotBand);

	updateSetConstructionTable();
};

var updateSetConstructionTable = function() {
	var tableHtml = '';

	for (var i = 0; i < flaggedObsRanges.length; ++i) {
		var flaggedRange = flaggedObsRanges[i];
		tableHtml += '<tr><td>' + flaggedRange.label.text + '</td>' +
		'<td>' + new Date(flaggedRange.from).toISOString() + '</td>' +
		'<td>' + new Date(flaggedRange.to).toISOString() + '</td>' +
		'<td>' + flaggedRange.obs_count + '</td>' +
		'<td>' + flaggedRange.file_count + '</td>' +
		'<td><button onclick=\'obs_file.unflagRange('' + flaggedRange.id +
		'')\'>Unflag range</button></td></tr>';
	}

	$('#set_construction_table > tbody').html(tableHtml);
};

var removedFlaggedRange = function(index) {
	removeAllPlotBands();
	reinsertDataForRange(index);
	flaggedObsRanges.splice(index, 1);
	flaggedFileRanges.splice(index, 1);
	updateFlaggedRangeIdsAndLabels();
	addAllPlotBands();
};

var unflagRange = function(flaggedRangeId) {
	for (var i = 0; i < flaggedObsRanges.length; ++i) {
		if (flaggedObsRanges[i].id === flaggedRangeId) {
			removedFlaggedRange(i);
			break;
		}
	}
	for (var i = 0; i < flaggedFileRanges.length; ++i) {
		if (flaggedFileRanges[i].id === flaggedRangeId) {
			removedFlaggedRange(i);
			break;
		}
	}

	updateSetConstructionTable();
};
dataSourceObj.unflagRange = unflagRange;

var reinsertDataForRange = function(index) {
	if ($('#remove_flagged_data_checkbox').is(':checked')) {
		var thisRange = flaggedObsRanges[index];
		var obsSeries = _chart.series[0];
		var fileSeries = _chart.series[1];
		{% if is_set %}
		var obsSeriesData = obs_counts;
		{% else %}
		var polarization = currentObsData['polarization'];
		var era_type = currentObsData['era_type'];
		var obsSeriesData = obs_counts[polarization][era_type];
		{% endif %}
		var fileSeriesData = file_counts;

		var obsSeriesGraphData = getSeriesDataCopyFromGraph(obsSeries);
		var fileSeriesGraphData = getSeriesDataCopyFromGraph(fileSeries);

		for (var obsIndex = thisRange.obs_start_index; obsIndex <= thisRange.obs_end_index; ++obsIndex) {
			obsSeriesGraphData[obsIndex]['obsnum'] = obsSeriesData[obsIndex]['obsnum'];
		}
		for (var fileIndex = thisRange.file_start_index; fileIndex <= thisRange.file_end_index; ++fileIndex) {
			fileSeriesGraphData[fileIndex]['obsnum'] = fileSeriesData[fileIndex]['obsnum'];
		}

		obsSeries.setData(obsSeriesGraphData, false);
		fileSeries.setData(fileSeriesGraphData, false);
	}
};

var clickConstructionModeCheckbox = function(checkbox) {
	inConstructionMode = checkbox.checked;
	if (inConstructionMode) { // Entering construction mode.
		$('#construction_controls').show(); // Show set construction controls.
		clickDragMode = $('#click_drag_dropdown').val();
		{% if not is_set %} // If we're looking at a set, the plot bands will always be on the graph.
			addAllPlotBands(); // Also, because the user can't change the data set, the table doesn't need to be updated.

			// Update the information in the table.
			updateSetConstructionTable();
		{% endif %}
	} else { // Exiting construction mode.
		$('#construction_controls').hide(); // Hide set construction controls.
		clickDragMode = 'zoom'; // Only allow zooming when not in construction mode.
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
	if ($('#remove_flagged_data_checkbox').is(':checked')) {
		var obsSeries = _chart.series[0];
		var fileSeries = _chart.series[1];
		var obsSeriesGraphData = getSeriesDataCopyFromGraph(obsSeries);
		var fileSeriesGraphData = getSeriesDataCopyFromGraph(fileSeries);

		for (var obsIndex = flaggedRange.obs_start_index; obsIndex <= flaggedRange.obs_end_index; ++obsIndex) {
			obsSeriesGraphData[obsIndex]['obsnum'] = null;
		}
		for (var fileIndex = flaggedRange.file_start_index; fileIndex <= flaggedRange.file_end_index; ++fileIndex) {
			fileSeriesGraphData[fileIndex]['obsnum'] = null;
		}

		obsSeries.setData(obsSeriesGraphData, false);
		fileSeries.setData(fileSeriesGraphData, false);
	}
};

var clickRemoveFlaggedDataCheckbox = function(checkbox) {
	var remove = checkbox.checked;

	var obsSeries = _chart.series[0];
	var fileSeries = _chart.series[1];

	if (!remove) {
		{% if is_set %}
		var obsSeriesData = obs_counts;
		var fileSeriesData = file_counts;
		{% else %}
		var polarization = currentObsData['polarization'];
		var era_type = currentObsData['era_type'];
		var obsSeriesData = obs_counts[polarization][era_type];
		var host = currentFileData['host'];
		var filetype = currentFileData['filetype'];
		var fileSeriesData = file_counts[host][filetype];
		{% endif %}
	}
	var obsSeriesGraphData = getSeriesDataCopyFromGraph(obsSeries);
	var fileSeriesGraphData = getSeriesDataCopyFromGraph(fileSeries);

	for (var flaggedRangeIndex = 0; flaggedRangeIndex < flaggedObsRanges.length; ++flaggedRangeIndex) {
		var thisRange = flaggedObsRanges[flaggedRangeIndex];
		for (var obsIndex = thisRange.obs_start_index; obsIndex <= thisRange.obs_end_index; ++obsIndex) {
			if (remove) {
				obsSeriesGraphData[obsIndex]['obsnum'] = null;
			} else {
				obsSeriesGraphData[obsIndex]['obsnum'] = obsSeriesData[obsIndex]['obsnum'];
			}
		}
		for (var fileIndex = thisRange.file_start_index; fileIndex <= thisRange.file_end_index; ++fileIndex) {
			if (remove) {
				fileSeriesGraphData[fileIndex]['obsnum'] = null;
			} else {
				fileSeriesGraphData[fileIndex]['obsnum'] = fileSeriesData[fileIndex]['obsnum'];
			}
		}
	}

	obsSeries.setData(obsSeriesGraphData, false);
	fileSeries.setData(fileSeriesGraphData, false);
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
