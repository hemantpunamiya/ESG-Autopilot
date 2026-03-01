from http.server import BaseHTTPRequestHandler
import json


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()

        results = {}

        try:
            import flask
            results["flask"] = "ok - " + flask.__version__
        except Exception as e:
            results["flask"] = "FAIL: " + str(e)

        try:
            import pandas
            results["pandas"] = "ok - " + pandas.__version__
        except Exception as e:
            results["pandas"] = "FAIL: " + str(e)

        try:
            import openpyxl
            results["openpyxl"] = "ok - " + openpyxl.__version__
        except Exception as e:
            results["openpyxl"] = "FAIL: " + str(e)

        try:
            import io, base64, re, os
            results["stdlib"] = "ok"
        except Exception as e:
            results["stdlib"] = "FAIL: " + str(e)

        self.wfile.write(json.dumps(results, indent=2).encode())
