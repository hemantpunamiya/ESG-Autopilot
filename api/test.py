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
        api_dir = os.path.dirname(os.path.abspath(__file__))

        # Try loading index.py as a module to see the exact error
        try:
            import importlib.util
            spec = importlib.util.spec_from_file_location(
                "index_test", os.path.join(api_dir, "index.py"))
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            results["index_load"] = "ok"
            results["has_app"] = hasattr(mod, "app")
            results["has_handler"] = hasattr(mod, "handler")
        except Exception as e:
            import traceback
            results["index_load"] = "FAIL"
            results["index_error"] = traceback.format_exc()

        self.wfile.write(json.dumps(results, indent=2).encode())
