const chart = LightweightCharts.createChart(document.getElementById('chart'), {
    width: 800,
    height: 600,
});
const candleSeries = chart.addCandlestickSeries();

async function fetchData() {
    const response = await fetch('/api/history?symbol=AAPL&from=2025-01-01T00:00:00&to=2025-04-01T00:00:00');
    const data = await response.json();
    candleSeries.setData(data.map(row => ({
        time: row[0],
        open: row[1],
        high: row[2],
        low: row[3],
        close: row[4],
    })));
}

fetchData();
