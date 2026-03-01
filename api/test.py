from http.server import BaseHTTPRequestHandler
import json
import sys
import os


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()

        results = {}

        # Check what files exist in the api/ directory
        api_dir = os.path.dirname(os.path.abspath(__file__))
        results["api_dir"] = api_dir
        try:
            results["files_in_api_dir"] = os.listdir(api_dir)
        except Exception as e:
            results["files_in_api_dir"] = "ERROR: " + str(e)

        # Try importing logic.py
        try:
            sys.path.insert(0, api_dir)
            import logic
            results["logic_import"] = "ok"
            results["logic_has_pd"] = hasattr(logic, "pd")
            results["logic_has_EF_DATABASE"] = hasattr(logic, "EF_DATABASE")
        except Exception as e:
            import traceback
            results["logic_import"] = "FAIL: " + traceback.format_exc()

        self.wfile.write(json.dumps(results, indent=2).encode())
