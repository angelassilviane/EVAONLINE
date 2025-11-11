window.dashExtensions = Object.assign({}, window.dashExtensions, {
    default: {
        function0: function(feature, latlng) {
            return L.circleMarker(latlng, {
                color: 'red',
                fillColor: '#f03',
                fillOpacity: 0.5,
                radius: 8
            });
        }

    }
});
