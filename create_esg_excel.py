import random
import os
try:
    import xlsxwriter
except ImportError:
    import subprocess
    import sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "xlsxwriter"])
    import xlsxwriter

# Define the file path for the output Excel workbook
output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'ESG_Master_Data_2024_2026.xlsx')

locations = ["Mumbai", "Delhi", "Bangalore", "Pune", "Chennai", "Hyderabad", "Kolkata", "Ahmedabad", "Surat", "Jaipur", 
             "Lucknow", "Kanpur", "Nagpur", "Indore", "Thane", "Bhopal", "Visakhapatnam", "Pimpri-Chinchwad", "Patna", "Vadodara"]

workbook = xlsxwriter.Workbook(output_path)
bold = workbook.add_format({'bold': True})
header_format = workbook.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'border': 1, 'text_wrap': True})
cell_format = workbook.add_format({'border': 1})
float_format = workbook.add_format({'border': 1, 'num_format': '#,##0.00'})
int_format = workbook.add_format({'border': 1, 'num_format': '0'})

# 1. Instructions
ws1 = workbook.add_worksheet('Instructions')
ws1.write('A1', 'Instructions for Data Entry', bold)
instructions = [
    "Each location must fill in their respective data.",
    "Please ensure the units match the specified format for accurate calculations.",
    "Units used:",
    "- HSD: KL",
    "- Natural Gas: SCM",
    "- LPG: Kg",
    "- Coal: t (tonnes)",
    "- Petrol/Diesel: L",
    "- Refrigerant Gas: Kg",
    "- Electricity: kWh",
    "- Bioethanol: L",
    "- Wood Pellets/Biogas: t (tonnes)"
]
for i, text in enumerate(instructions):
    ws1.write(i+2, 0, text)
ws1.set_column('A:A', 80)

# 2. Scope 1 Stationary
ws2 = workbook.add_worksheet('Scope 1 Stationary Combustion')
ws2.merge_range('A1:A2', 'Location', header_format)
ws2.merge_range('B1:E1', 'FY 2024-25', header_format)
ws2.merge_range('F1:I1', 'FY 2025-26', header_format)
fuels_stat = ['HSD (KL)', 'Natural Gas (SCM)', 'LPG (Kg)', 'Coal (t)']
for col_num, fuel in enumerate(fuels_stat):
    ws2.write(1, 1 + col_num, fuel, header_format)
    ws2.write(1, 5 + col_num, fuel, header_format)

for row_num, loc in enumerate(locations):
    ws2.write(row_num + 2, 0, loc, cell_format)
    for fy_offset in [0, 4]:
        ws2.write(row_num + 2, 1 + fy_offset, round(random.uniform(5, 50), 2), float_format)
        ws2.write(row_num + 2, 2 + fy_offset, round(random.uniform(500, 3000), 2), float_format)
        ws2.write(row_num + 2, 3 + fy_offset, random.choice([0, 0, round(random.uniform(50, 300), 2)]), float_format)
        ws2.write(row_num + 2, 4 + fy_offset, random.choice([0, 0, 0, round(random.uniform(10, 50), 2)]), float_format)
ws2.set_column('A:A', 20)
ws2.set_column('B:I', 18)

# 3. Scope 1 Mobile Combustion
ws3 = workbook.add_worksheet('Scope 1 Mobile Combustion')
ws3.merge_range('A1:A2', 'Location', header_format)
ws3.merge_range('B1:C1', 'FY 2024-25', header_format)
ws3.merge_range('D1:E1', 'FY 2025-26', header_format)
fuels_mob = ['Petrol (L)', 'Diesel (L)']
for col_num, fuel in enumerate(fuels_mob):
    ws3.write(1, 1 + col_num, fuel, header_format)
    ws3.write(1, 3 + col_num, fuel, header_format)

for row_num, loc in enumerate(locations):
    ws3.write(row_num + 2, 0, loc, cell_format)
    for fy_offset in [0, 2]:
        ws3.write(row_num + 2, 1 + fy_offset, round(random.uniform(100, 2000), 2), float_format)
        ws3.write(row_num + 2, 2 + fy_offset, round(random.uniform(500, 5000), 2), float_format)
ws3.set_column('A:A', 20)
ws3.set_column('B:E', 15)

# 4. Scope 1 Fugitive Emissions
ws4 = workbook.add_worksheet('Scope 1 Fugitive Emissions')
# Table 1: Refrigerant Gas Refills
ws4.write('A1', 'Table 1: Refrigerant Gas Refills (Kg)', bold)
ws4.merge_range('A2:A3', 'Location', header_format)
ws4.merge_range('B2:D2', 'FY 2024-25', header_format)
ws4.merge_range('E2:G2', 'FY 2025-26', header_format)
refrigerants = ['R22', 'R134a', 'R410A']
for c, ref in enumerate(refrigerants):
    ws4.write(2, 1 + c, ref, header_format)
    ws4.write(2, 4 + c, ref, header_format)

for r, loc in enumerate(locations):
    ws4.write(r + 3, 0, loc, cell_format)
    for fy_offset in [0, 3]:
        ws4.write(r + 3, 1 + fy_offset, random.choice([0, round(random.uniform(2, 10), 2)]), float_format)
        ws4.write(r + 3, 2 + fy_offset, random.choice([0, 0, round(random.uniform(5, 20), 2)]), float_format)
        ws4.write(r + 3, 3 + fy_offset, random.choice([0, round(random.uniform(10, 50), 2)]), float_format)

# Table 2: CO2 Fire Extinguisher Refills
start_row = len(locations) + 5
ws4.write(start_row, 0, 'Table 2: CO2 Fire Extinguisher Refills (Counts)', bold)
ws4.merge_range(start_row+1, 0, start_row+2, 0, 'Location', header_format)
ws4.merge_range(start_row+1, 1, start_row+1, 4, 'FY 2024-25', header_format)
ws4.merge_range(start_row+1, 5, start_row+1, 8, 'FY 2025-26', header_format)
ext_sizes = ['2kg', '4.5kg', '6kg', '9kg']
for c, size in enumerate(ext_sizes):
    ws4.write(start_row+2, 1 + c, size, header_format)
    ws4.write(start_row+2, 5 + c, size, header_format)

for r, loc in enumerate(locations):
    ws4.write(start_row + 3 + r, 0, loc, cell_format)
    for fy_offset in [0, 4]:
        ws4.write(start_row + 3 + r, 1 + fy_offset, random.randint(0, 5), int_format)
        ws4.write(start_row + 3 + r, 2 + fy_offset, random.randint(0, 3), int_format)
        ws4.write(start_row + 3 + r, 3 + fy_offset, random.randint(0, 2), int_format)
        ws4.write(start_row + 3 + r, 4 + fy_offset, random.randint(0, 1), int_format)
        
ws4.set_column('A:A', 20)
ws4.set_column('B:I', 12)

# 5. Electricity
ws5 = workbook.add_worksheet('Electricity')
ws5.merge_range('A1:A2', 'Location', header_format)
ws5.merge_range('B1:D1', 'FY 2024-25', header_format)
ws5.merge_range('E1:G1', 'FY 2025-26', header_format)
elec_cols = ['Total kWh', 'NRE (Grid)', 'RE (Solar)']
for c, el in enumerate(elec_cols):
    ws5.write(1, 1 + c, el, header_format)
    ws5.write(1, 4 + c, el, header_format)

for r, loc in enumerate(locations):
    ws5.write(r + 2, 0, loc, cell_format)
    for fy_offset in [0, 3]:
        total = round(random.uniform(100000, 500000), 2)
        re = round(total * random.uniform(0.1, 0.4), 2)
        nre = round(total - re, 2)
        ws5.write(r + 2, 1 + fy_offset, total, float_format)
        ws5.write(r + 2, 2 + fy_offset, nre, float_format)
        ws5.write(r + 2, 3 + fy_offset, re, float_format)
ws5.set_column('A:A', 20)
ws5.set_column('B:G', 15)

# 6. Bioenergy
ws6 = workbook.add_worksheet('Bioenergy')
ws6.merge_range('A1:A2', 'Location', header_format)
ws6.merge_range('B1:D1', 'FY 2024-25', header_format)
ws6.merge_range('E1:G1', 'FY 2025-26', header_format)
bio_cols = ['Bioethanol (L)', 'Wood Pellets (t)', 'Biogas (t)']
for c, el in enumerate(bio_cols):
    ws6.write(1, 1 + c, el, header_format)
    ws6.write(1, 4 + c, el, header_format)

for r, loc in enumerate(locations):
    ws6.write(r + 2, 0, loc, cell_format)
    for fy_offset in [0, 3]:
        ws6.write(r + 2, 1 + fy_offset, random.choice([0, 0, round(random.uniform(100, 1000), 2)]), float_format)
        ws6.write(r + 2, 2 + fy_offset, random.choice([0, 0, round(random.uniform(5, 50), 2)]), float_format)
        ws6.write(r + 2, 3 + fy_offset, random.choice([0, 0, 0, round(random.uniform(1, 10), 2)]), float_format)
ws6.set_column('A:A', 20)
ws6.set_column('B:G', 16)

workbook.close()
print(f"Excel file generated successfully at {output_path}")
