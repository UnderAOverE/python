from fastapi import FastAPI, Response
from typing import List

app = FastAPI()

# Simulated data for multiple workers
workers_data = [
    {
        "name": "Raphael",
        "host": "host1",
        "last_heartbeat": "2025-01-29T19:46:20.438+00:00",
        "metrics": {
            "mongo_connection": {"application": 8, "current": 12, "available": 121212},
            "process": {"name": "python", "pid": 180944, "process_cpu_percent": 0, "process_memeory": 12},
            "system": {
                "system_cpu_percent": 32,
                "memory_usage": {"total": 23, "used": 1, "free": 123, "percent": 2},
            },
        },
        "process_pid": 180944,
        "status": "running",
    },
    {
        "name": "Donatello",
        "host": "host2",
        "last_heartbeat": "2025-01-29T19:48:10.112+00:00",
        "metrics": {
            "mongo_connection": {"application": 5, "current": 7, "available": 111111},
            "process": {"name": "python", "pid": 180945, "process_cpu_percent": 1, "process_memeory": 18},
            "system": {
                "system_cpu_percent": 30,
                "memory_usage": {"total": 28, "used": 4, "free": 100, "percent": 14},
            },
        },
        "process_pid": 180945,
        "status": "running",
    }
]

def generate_table_html(data: List[dict]) -> str:
    """
    Generate HTML table for worker metrics.
    """
    rows = ""
    for idx, worker in enumerate(data, start=1):
        rows += f"""
            <tr>
                <td>{idx}</td>
                <td>{worker["name"]}</td>
                <td>{worker["host"]}</td>
                <td>{worker["status"]}</td>
                <td>{worker["metrics"]["mongo_connection"]["current"]}</td>
                <td>{worker["metrics"]["process"]["process_memeory"]} MB</td>
                <td>{worker["metrics"]["system"]["system_cpu_percent"]}%</td>
                <td>{worker["last_heartbeat"]}</td>
            </tr>
        """
    return rows


@app.get("/health")
def health_check():
    """
    Display worker health metrics in paginated HTML.
    """
    table_content = generate_table_html(workers_data)
    html_content = f"""
    <html>
        <head>
            <title>Health Metrics</title>
            <style>
                table {{
                    width: 80%;
                    border-collapse: collapse;
                    margin: 20px auto;
                }}
                th, td {{
                    border: 1px solid #ddd;
                    padding: 8px;
                    text-align: left;
                }}
                th {{
                    background-color: #f2f2f2;
                }}
                .pagination {{
                    text-align: center;
                    margin-top: 20px;
                }}
                .pagination a {{
                    color: black;
                    padding: 8px 16px;
                    text-decoration: none;
                    border: 1px solid #ddd;
                }}
                .pagination a.active {{
                    background-color: #04AA6D;
                    color: white;
                }}
            </style>
            <script>
                let currentPage = 1;
                const rowsPerPage = 2;
                let rows = document.querySelectorAll("tbody tr");

                function displayTable(page) {{
                    rows.forEach((row, index) => {{
                        row.style.display = index >= (page - 1) * rowsPerPage && index < page * rowsPerPage
                            ? "table-row"
                            : "none";
                    }});
                }}

                function changePage(page) {{
                    currentPage = page;
                    displayTable(page);
                }}
            </script>
        </head>
        <body>
            <h1 style="text-align: center;">Worker Health Metrics</h1>
            <table>
                <thead>
                    <tr>
                        <th>#</th>
                        <th>Name</th>
                        <th>Host</th>
                        <th>Status</th>
                        <th>MongoDB Connections</th>
                        <th>Memory Usage</th>
                        <th>CPU Usage</th>
                        <th>Last Heartbeat</th>
                    </tr>
                </thead>
                <tbody>
                    {table_content}
                </tbody>
            </table>
            <div class="pagination">
                <a href="#" onclick="changePage(1)" class="active">1</a>
                <a href="#" onclick="changePage(2)">2</a>
            </div>
        </body>
    </html>
    """
    return Response(content=html_content, media_type="text/html")
