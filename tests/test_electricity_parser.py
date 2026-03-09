import unittest

import pandas as pd

from api.logic import process_electricity_sheet


class ElectricityParserTests(unittest.TestCase):
    def test_parses_grouped_total_nre_re_headers(self):
        raw = pd.DataFrame([
            ["Location / Plant", "FY 2024-25", None, None, "FY 2025-26", None, None],
            [None, "Total kWh", "NRE (Grid)", "RE (Solar)", "Total kWh", "NRE (Grid)", "RE (Solar)"],
            ["Mumbai", 374157.32, 295429.78, 78727.54, 285704.21, 256798.48, 28905.73],
            ["Delhi", 134733.43, 114488.67, 20244.76, 310083.82, 203141.59, 106942.23],
        ])

        rows = process_electricity_sheet(raw)

        self.assertEqual(len(rows), 8)
        self.assertEqual(sum(r["Fuel / Electricity Type"] == "Grid Electricity (kWh)" for r in rows), 4)
        self.assertEqual(sum(r["Fuel / Electricity Type"] == "Solar (kWh)" for r in rows), 4)
        self.assertEqual({r["Location"] for r in rows}, {"Mumbai", "Delhi"})
        self.assertEqual({r["Period"] for r in rows}, {"FY 2024-25", "FY 2025-26"})


if __name__ == "__main__":
    unittest.main()
