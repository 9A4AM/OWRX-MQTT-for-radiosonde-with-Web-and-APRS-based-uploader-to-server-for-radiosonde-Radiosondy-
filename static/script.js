function loadData() {
    fetch("/data")
    .then(response => response.json())
    .then(data => {
        let table = document.getElementById("data-table");
        table.innerHTML = "";
        data.forEach(entry => {
            let row = `<tr>
                <td>${entry.ser}</td>
                <td>${entry.lat}</td>
                <td>${entry.lon}</td>
                <td>${entry.alt}</td>
                <td>${entry.speed}</td>
                <td>${entry.dir}</td>
                <td>${entry.type}</td>
                <td>${entry.sats}</td>
                <td>${entry.freq}</td>
                <td>${entry.rssi}</td>
            </tr>`;
            table.innerHTML += row;
        });
    });
}

setInterval(loadData, 5000);
loadData();
