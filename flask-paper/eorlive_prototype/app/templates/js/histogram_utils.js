var getColumnRangeLimits = function(event, series) {
    var absoluteIndex = event.point.index, relativeIndex = 0;

    for (var i = 0; i < series.points.length; ++i) {
        if (series.points[i].index === absoluteIndex) {
            relativeIndex = i;
            break;
        }
    }

    var startTime = series.processedXData[relativeIndex], endTime = 0;

    if (series.closestPointRange !== undefined) {
        endTime = startTime + series.closestPointRange;
    } else if (series.processedXData.length > relativeIndex + 1) {
        endTime = series.processedXData[relativeIndex + 1];
    } else if (series.xData.length > absoluteIndex + 1) {
        endTime = series.xData[absoluteIndex + 1];
    } else {
        endTime = Date.parse("{{ range_end }}");
    }

    return { startTime: startTime, endTime: endTime };
};
