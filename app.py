import streamlit as st
import io
import pandas as pd
import re
from datetime import datetime

# Set page configuration
st.set_page_config(page_title="ESG AutoPilot - Enterprise", page_icon="💎", layout="wide")

# CSS for SaaS Aesthetics
st.markdown("""
<style>
    .stApp {
        background:
            radial-gradient(circle at 8% 8%, rgba(16,185,129,0.10) 0%, rgba(16,185,129,0) 30%),
            radial-gradient(circle at 90% 12%, rgba(14,116,144,0.10) 0%, rgba(14,116,144,0) 35%),
            #f4f7fb;
    }
    .stMetric {
        background: #ffffff;
        padding: 16px;
        border-radius: 14px;
        border: 1px solid #d8e2ee;
        box-shadow: 0 6px 20px rgba(16, 24, 40, 0.06);
    }
    .kpi-title {
        font-size: 0.92rem;
        color: #375a7f;
        letter-spacing: 0.02em;
        font-weight: 700;
        margin-bottom: 0.2rem;
        text-transform: uppercase;
    }
    .hero {
        background: linear-gradient(135deg, #0f766e 0%, #155e75 100%);
        color: white;
        padding: 16px 18px;
        border-radius: 16px;
        margin-bottom: 0.8rem;
    }
    .hero h2 { margin: 0 0 4px 0; font-size: 1.35rem; }
    .hero p { margin: 0; opacity: 0.9; font-size: 0.92rem; }
    .highlight-card {
        background: #ffffff;
        border: 1px solid #d8e2ee;
        border-radius: 12px;
        padding: 12px 14px;
        margin-bottom: 10px;
        box-shadow: 0 4px 12px rgba(16,24,40,0.05);
    }
    .highlight-card .label {
        color: #44617f;
        font-size: 0.82rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.02em;
    }
    .highlight-card .value {
        color: #0f172a;
        font-size: 1.35rem;
        font-weight: 800;
        margin-top: 2px;
    }
    .highlight-card .delta {
        color: #1d4ed8;
        font-size: 0.78rem;
        margin-top: 2px;
    }
    .sub-indent { margin-left: 20px; }
    div[data-testid="stExpander"] {
        background-color: white;
        border-radius: 12px;
        border: 1px solid #dbe4f0;
    }
</style>
""", unsafe_allow_html=True)

# --- Configuration & Master Data ---
STANDARD_COLUMNS = [
    "Period", "Location", "Site Type", "Scope", "Category",
    "Fuel / Electricity Type", "Fuel Type", "Quantity", "Unit", "EF Original Unit",
    "Unit Adjusted EF", "Energy Usage (GJ)", "Total Emissions (kgCO2e)",
    "Total Emissions (tCO2e)", "Factor Source", "Methodology", "Validation Notes"
]

EF_DATABASE = {
    # Electricity (Scope 2)
    "Grid Electricity (kWh)":      {"factor": 0.71,  "ncv": 0.0036, "unit": "kWh", "fuel_type": "Electricity",    "is_renewable": False, "source": "CEA India 2023",       "methodology": "Scope 2 - Grid Avg",              "scope": "Scope 2",  "category": "Grid Electricity"},
    "Renewable Electricity (kWh)": {"factor": 0.0,   "ncv": 0.0036, "unit": "kWh", "fuel_type": "Electricity",    "is_renewable": True,  "source": "Renewable",            "methodology": "Zero Emission",                   "scope": "Scope 2",  "category": "Renewable Energy"},
    "Solar (kWh)":                 {"factor": 0.0,   "ncv": 0.0036, "unit": "kWh", "fuel_type": "Electricity",    "is_renewable": True,  "source": "Renewable",            "methodology": "Zero Emission",                   "scope": "Scope 2",  "category": "Renewable Energy"},
    "Wind (kWh)":                  {"factor": 0.0,   "ncv": 0.0036, "unit": "kWh", "fuel_type": "Electricity",    "is_renewable": True,  "source": "Renewable",            "methodology": "Zero Emission",                   "scope": "Scope 2",  "category": "Renewable Energy"},
    "Hydel (kWh)":                 {"factor": 0.0,   "ncv": 0.0036, "unit": "kWh", "fuel_type": "Electricity",    "is_renewable": True,  "source": "Renewable",            "methodology": "Zero Emission",                   "scope": "Scope 2",  "category": "Renewable Energy"},

    # Fuels - Stationary (IPCC 2006, AR5 GWPs: CO2=1, CH4=27, N2O=273)
    # EF = Density(t/KL) x NCV(GJ/t) x [CO2_EF(kg/TJ)/1000 + CH4_EF(kg/TJ)/1000*27 + N2O_EF(kg/TJ)/1000*273]
    # NCV here = energy per unit of fuel (GJ/KL or GJ/kg or GJ/SCM)
    "HSD (KL)":          {"factor": 2701.3,  "ncv": 36.335,  "unit": "KL",  "fuel_type": "Liquid",   "is_renewable": False, "source": "IPCC 2006, AR5 GWPs", "methodology": "Scope 1 - Stationary Combustion", "scope": "Scope 1", "category": "Stationary Combustion"},
    "Furnace Oil (KL)":  {"factor": 3010.0,  "ncv": 38.784,  "unit": "KL",  "fuel_type": "Liquid",   "is_renewable": False, "source": "IPCC 2006, AR5 GWPs", "methodology": "Scope 1 - Stationary Combustion", "scope": "Scope 1", "category": "Stationary Combustion"},
    "LDO (KL)":          {"factor": 2749.3,  "ncv": 36.98,   "unit": "KL",  "fuel_type": "Liquid",   "is_renewable": False, "source": "IPCC 2006, AR5 GWPs", "methodology": "Scope 1 - Stationary Combustion", "scope": "Scope 1", "category": "Stationary Combustion"},
    "Natural Gas (SCM)": {"factor": 2.15,    "ncv": 0.0384,  "unit": "SCM", "fuel_type": "Gaseous",  "is_renewable": False, "source": "IPCC 2006, AR5 GWPs", "methodology": "Scope 1 - Stationary Combustion", "scope": "Scope 1", "category": "Stationary Combustion"},
    "LPG (Kg)":          {"factor": 2.987,   "ncv": 0.0473,  "unit": "kg",  "fuel_type": "Gaseous",  "is_renewable": False, "source": "IPCC 2006, AR5 GWPs", "methodology": "Scope 1 - Stationary Combustion", "scope": "Scope 1", "category": "Stationary Combustion"},
    "LSHS (KL)":         {"factor": 3042.7,  "ncv": 39.188,  "unit": "KL",  "fuel_type": "Liquid",   "is_renewable": False, "source": "IPCC 2006, AR5 GWPs", "methodology": "Scope 1 - Stationary Combustion", "scope": "Scope 1", "category": "Stationary Combustion"},

    # Fuels - Mobile (IPCC 2006, AR5 GWPs - higher CH4/N2O for mobile combustion)
    "HSD Mobile (KL)":   {"factor": 2734.9,  "ncv": 36.335,  "unit": "KL",  "fuel_type": "Liquid",   "is_renewable": False, "source": "IPCC 2006, AR5 GWPs", "methodology": "Scope 1 - Mobile Combustion",     "scope": "Scope 1", "category": "Mobile Combustion"},
    "Petrol (KL)":       {"factor": 2341.9,  "ncv": 33.004,  "unit": "KL",  "fuel_type": "Liquid",   "is_renewable": False, "source": "IPCC 2006, AR5 GWPs", "methodology": "Scope 1 - Mobile Combustion",     "scope": "Scope 1", "category": "Mobile Combustion"},

    # Biogenic Fuels (reported separately per GHG Protocol)
    "Biofuel (KL)":      {"factor": 1688.5,  "ncv": 23.76,   "unit": "KL",  "fuel_type": "Liquid Biofuel",  "is_renewable": False, "source": "IPCC 2006, Biogenic", "methodology": "Biogenic - Reported Separately", "scope": "Biogenic", "category": "Biogenic Emissions"},
    "Biodiesel (KL)":    {"factor": 1688.5,  "ncv": 23.76,   "unit": "KL",  "fuel_type": "Liquid Biofuel",  "is_renewable": False, "source": "IPCC 2006, Biogenic", "methodology": "Biogenic - Reported Separately", "scope": "Biogenic", "category": "Biogenic Emissions"},
    "Briquettes (Kg)":   {"factor": 1.592,   "ncv": 0.0156,  "unit": "kg",  "fuel_type": "Solid Biomass",   "is_renewable": False, "source": "IPCC 2006, Biogenic", "methodology": "Biogenic - Reported Separately", "scope": "Biogenic", "category": "Biogenic Emissions"},

    # Refrigerants (GWPs updated to AR6 / source data values)
    "R22 (kg)":    {"factor": 1960.0,  "ncv": 0.0, "unit": "kg", "fuel_type": "Refrigerant", "is_renewable": False, "source": "IPCC AR6", "methodology": "Scope 1 - Fugitive Emissions", "scope": "Scope 1", "category": "Refrigerants"},
    "R134a (kg)":  {"factor": 1530.0,  "ncv": 0.0, "unit": "kg", "fuel_type": "Refrigerant", "is_renewable": False, "source": "IPCC AR6", "methodology": "Scope 1 - Fugitive Emissions", "scope": "Scope 1", "category": "Refrigerants"},
    "R-407C (kg)": {"factor": 1908.0,  "ncv": 0.0, "unit": "kg", "fuel_type": "Refrigerant", "is_renewable": False, "source": "IPCC AR6", "methodology": "Scope 1 - Fugitive Emissions", "scope": "Scope 1", "category": "Refrigerants"},
    "R-404A (kg)": {"factor": 4728.0,  "ncv": 0.0, "unit": "kg", "fuel_type": "Refrigerant", "is_renewable": False, "source": "IPCC AR6", "methodology": "Scope 1 - Fugitive Emissions", "scope": "Scope 1", "category": "Refrigerants"},
    "R-410A (kg)": {"factor": 2255.0,  "ncv": 0.0, "unit": "kg", "fuel_type": "Refrigerant", "is_renewable": False, "source": "IPCC AR6", "methodology": "Scope 1 - Fugitive Emissions", "scope": "Scope 1", "category": "Refrigerants"},
    "R-407A (kg)": {"factor": 2262.0,  "ncv": 0.0, "unit": "kg", "fuel_type": "Refrigerant", "is_renewable": False, "source": "IPCC AR6", "methodology": "Scope 1 - Fugitive Emissions", "scope": "Scope 1", "category": "Refrigerants"},
    "R123 (kg)":   {"factor": 79.0,    "ncv": 0.0, "unit": "kg", "fuel_type": "Refrigerant", "is_renewable": False, "source": "IPCC AR6", "methodology": "Scope 1 - Fugitive Emissions", "scope": "Scope 1", "category": "Refrigerants"},
    "R32 (kg)":    {"factor": 771.0,   "ncv": 0.0, "unit": "kg", "fuel_type": "Refrigerant", "is_renewable": False, "source": "IPCC AR6", "methodology": "Scope 1 - Fugitive Emissions", "scope": "Scope 1", "category": "Refrigerants"},
    "R152a (kg)":  {"factor": 164.0,   "ncv": 0.0, "unit": "kg", "fuel_type": "Refrigerant", "is_renewable": False, "source": "IPCC AR6", "methodology": "Scope 1 - Fugitive Emissions", "scope": "Scope 1", "category": "Refrigerants"},

    # Fire Extinguishers (CO2 Refills - factor = capacity in kg)
    "Fire Extinguisher 1kg (nos)":    {"factor": 1.0,   "ncv": 0.0, "unit": "nos", "fuel_type": "Refrigerant", "is_renewable": False, "source": "CO2 Volume", "methodology": "Scope 1 - Fugitive Emissions", "scope": "Scope 1", "category": "Fire Extinguishers"},
    "Fire Extinguisher 2kg (nos)":    {"factor": 2.0,   "ncv": 0.0, "unit": "nos", "fuel_type": "Refrigerant", "is_renewable": False, "source": "CO2 Volume", "methodology": "Scope 1 - Fugitive Emissions", "scope": "Scope 1", "category": "Fire Extinguishers"},
    "Fire Extinguisher 2.5kg (nos)":  {"factor": 2.5,   "ncv": 0.0, "unit": "nos", "fuel_type": "Refrigerant", "is_renewable": False, "source": "CO2 Volume", "methodology": "Scope 1 - Fugitive Emissions", "scope": "Scope 1", "category": "Fire Extinguishers"},
    "Fire Extinguisher 3kg (nos)":    {"factor": 3.0,   "ncv": 0.0, "unit": "nos", "fuel_type": "Refrigerant", "is_renewable": False, "source": "CO2 Volume", "methodology": "Scope 1 - Fugitive Emissions", "scope": "Scope 1", "category": "Fire Extinguishers"},
    "Fire Extinguisher 3.5kg (nos)":  {"factor": 3.5,   "ncv": 0.0, "unit": "nos", "fuel_type": "Refrigerant", "is_renewable": False, "source": "CO2 Volume", "methodology": "Scope 1 - Fugitive Emissions", "scope": "Scope 1", "category": "Fire Extinguishers"},
    "Fire Extinguisher 4.5kg (nos)":  {"factor": 4.5,   "ncv": 0.0, "unit": "nos", "fuel_type": "Refrigerant", "is_renewable": False, "source": "CO2 Volume", "methodology": "Scope 1 - Fugitive Emissions", "scope": "Scope 1", "category": "Fire Extinguishers"},
    "Fire Extinguisher 6kg (nos)":    {"factor": 6.0,   "ncv": 0.0, "unit": "nos", "fuel_type": "Refrigerant", "is_renewable": False, "source": "CO2 Volume", "methodology": "Scope 1 - Fugitive Emissions", "scope": "Scope 1", "category": "Fire Extinguishers"},
    "Fire Extinguisher 9kg (nos)":    {"factor": 9.0,   "ncv": 0.0, "unit": "nos", "fuel_type": "Refrigerant", "is_renewable": False, "source": "CO2 Volume", "methodology": "Scope 1 - Fugitive Emissions", "scope": "Scope 1", "category": "Fire Extinguishers"},
    "Fire Extinguisher 22.5kg (nos)": {"factor": 22.5,  "ncv": 0.0, "unit": "nos", "fuel_type": "Refrigerant", "is_renewable": False, "source": "CO2 Volume", "methodology": "Scope 1 - Fugitive Emissions", "scope": "Scope 1", "category": "Fire Extinguishers"},

    # --- Additional Fuels: IPCC AR6 / UK DESNZ 2024 ---
    # Gaseous Fuels (L = litres; factors kgCO2e/L; NCV GJ/L)
    "CNG (L)":                           {"factor": 0.44423, "ncv": 0.00884, "unit": "L",  "fuel_type": "Gaseous",         "is_renewable": False, "source": "IPCC AR6, UK DESNZ 2024", "methodology": "Scope 1 - Stationary Combustion", "scope": "Scope 1",  "category": "Stationary Combustion"},
    "LNG (L)":                           {"factor": 1.15623, "ncv": 0.02200, "unit": "L",  "fuel_type": "Gaseous",         "is_renewable": False, "source": "IPCC AR6, UK DESNZ 2024", "methodology": "Scope 1 - Stationary Combustion", "scope": "Scope 1",  "category": "Stationary Combustion"},
    "LPG (L)":                           {"factor": 1.55709, "ncv": 0.02540, "unit": "L",  "fuel_type": "Gaseous",         "is_renewable": False, "source": "IPCC AR6, UK DESNZ 2024", "methodology": "Scope 1 - Stationary Combustion", "scope": "Scope 1",  "category": "Stationary Combustion"},
    "Natural Gas (m\u00b3)":             {"factor": 2.02135, "ncv": 0.03840, "unit": "m\u00b3", "fuel_type": "Gaseous",   "is_renewable": False, "source": "IPCC AR6, UK DESNZ 2024", "methodology": "Scope 1 - Stationary Combustion", "scope": "Scope 1",  "category": "Stationary Combustion"},
    "Natural Gas 100% Mineral (m\u00b3)":{"factor": 2.03473, "ncv": 0.03840, "unit": "m\u00b3", "fuel_type": "Gaseous",   "is_renewable": False, "source": "IPCC AR6, UK DESNZ 2024", "methodology": "Scope 1 - Stationary Combustion", "scope": "Scope 1",  "category": "Stationary Combustion"},
    "Other Petroleum Gas (L)":           {"factor": 0.94441, "ncv": 0.02500, "unit": "L",  "fuel_type": "Gaseous",         "is_renewable": False, "source": "IPCC AR6, UK DESNZ 2024", "methodology": "Scope 1 - Stationary Combustion", "scope": "Scope 1",  "category": "Stationary Combustion"},

    # Liquid Fuels (L = litres; factors kgCO2e/L; NCV GJ/L)
    "Aviation Spirit (L)":               {"factor": 2.33048, "ncv": 0.03470, "unit": "L",  "fuel_type": "Liquid",          "is_renewable": False, "source": "IPCC AR6, UK DESNZ 2024", "methodology": "Scope 1 - Mobile Combustion",     "scope": "Scope 1",  "category": "Mobile Combustion"},
    "Aviation Turbine Fuel (L)":         {"factor": 2.54514, "ncv": 0.03470, "unit": "L",  "fuel_type": "Liquid",          "is_renewable": False, "source": "IPCC AR6, UK DESNZ 2024", "methodology": "Scope 1 - Mobile Combustion",     "scope": "Scope 1",  "category": "Mobile Combustion"},
    "Burning Oil (L)":                   {"factor": 2.54014, "ncv": 0.03470, "unit": "L",  "fuel_type": "Liquid",          "is_renewable": False, "source": "IPCC AR6, UK DESNZ 2024", "methodology": "Scope 1 - Stationary Combustion", "scope": "Scope 1",  "category": "Stationary Combustion"},
    "Diesel Average Biofuel Blend (L)":  {"factor": 2.51233, "ncv": 0.03590, "unit": "L",  "fuel_type": "Liquid",          "is_renewable": False, "source": "IPCC AR6, UK DESNZ 2024", "methodology": "Scope 1 - Mobile Combustion",     "scope": "Scope 1",  "category": "Mobile Combustion"},
    "Diesel 100% Mineral (L)":           {"factor": 2.70553, "ncv": 0.03590, "unit": "L",  "fuel_type": "Liquid",          "is_renewable": False, "source": "IPCC AR6, UK DESNZ 2024", "methodology": "Scope 1 - Mobile Combustion",     "scope": "Scope 1",  "category": "Mobile Combustion"},
    "Fuel Oil (L)":                      {"factor": 3.17522, "ncv": 0.03870, "unit": "L",  "fuel_type": "Liquid",          "is_renewable": False, "source": "IPCC AR6, UK DESNZ 2024", "methodology": "Scope 1 - Stationary Combustion", "scope": "Scope 1",  "category": "Stationary Combustion"},
    "Gas Oil (L)":                       {"factor": 2.75857, "ncv": 0.03730, "unit": "L",  "fuel_type": "Liquid",          "is_renewable": False, "source": "IPCC AR6, UK DESNZ 2024", "methodology": "Scope 1 - Stationary Combustion", "scope": "Scope 1",  "category": "Stationary Combustion"},
    "Lubricants (L)":                    {"factor": 2.74972, "ncv": 0.03670, "unit": "L",  "fuel_type": "Liquid",          "is_renewable": False, "source": "IPCC AR6, UK DESNZ 2024", "methodology": "Scope 1 - Stationary Combustion", "scope": "Scope 1",  "category": "Stationary Combustion"},
    "Naphtha (L)":                       {"factor": 2.11926, "ncv": 0.03390, "unit": "L",  "fuel_type": "Liquid",          "is_renewable": False, "source": "IPCC AR6, UK DESNZ 2024", "methodology": "Scope 1 - Stationary Combustion", "scope": "Scope 1",  "category": "Stationary Combustion"},
    "Petrol Average Biofuel Blend (L)":  {"factor": 2.19352, "ncv": 0.03300, "unit": "L",  "fuel_type": "Liquid",          "is_renewable": False, "source": "IPCC AR6, UK DESNZ 2024", "methodology": "Scope 1 - Mobile Combustion",     "scope": "Scope 1",  "category": "Mobile Combustion"},
    "Petrol 100% Mineral (L)":           {"factor": 2.33969, "ncv": 0.03300, "unit": "L",  "fuel_type": "Liquid",          "is_renewable": False, "source": "IPCC AR6, UK DESNZ 2024", "methodology": "Scope 1 - Mobile Combustion",     "scope": "Scope 1",  "category": "Mobile Combustion"},
    "Processed Fuel Oil Residual (L)":   {"factor": 3.17522, "ncv": 0.03870, "unit": "L",  "fuel_type": "Liquid",          "is_renewable": False, "source": "IPCC AR6, UK DESNZ 2024", "methodology": "Scope 1 - Stationary Combustion", "scope": "Scope 1",  "category": "Stationary Combustion"},
    "Processed Fuel Oil Distillate (L)": {"factor": 2.75857, "ncv": 0.03730, "unit": "L",  "fuel_type": "Liquid",          "is_renewable": False, "source": "IPCC AR6, UK DESNZ 2024", "methodology": "Scope 1 - Stationary Combustion", "scope": "Scope 1",  "category": "Stationary Combustion"},
    "Waste Oils (L)":                    {"factor": 2.75368, "ncv": 0.03670, "unit": "L",  "fuel_type": "Liquid",          "is_renewable": False, "source": "IPCC AR6, UK DESNZ 2024", "methodology": "Scope 1 - Stationary Combustion", "scope": "Scope 1",  "category": "Stationary Combustion"},
    "Marine Gas Oil (L)":                {"factor": 2.77539, "ncv": 0.03730, "unit": "L",  "fuel_type": "Liquid",          "is_renewable": False, "source": "IPCC AR6, UK DESNZ 2024", "methodology": "Scope 1 - Stationary Combustion", "scope": "Scope 1",  "category": "Stationary Combustion"},
    "Marine Fuel Oil (L)":               {"factor": 3.10669, "ncv": 0.03870, "unit": "L",  "fuel_type": "Liquid",          "is_renewable": False, "source": "IPCC AR6, UK DESNZ 2024", "methodology": "Scope 1 - Stationary Combustion", "scope": "Scope 1",  "category": "Stationary Combustion"},

    # Solid Fuels (t = metric tonne; factors kgCO2e/t; NCV GJ/t)
    "Coal Industrial (t)":               {"factor": 2403.84, "ncv": 25.80,   "unit": "t",  "fuel_type": "Solid",           "is_renewable": False, "source": "IPCC AR6, UK DESNZ 2024", "methodology": "Scope 1 - Stationary Combustion", "scope": "Scope 1",  "category": "Stationary Combustion"},
    "Coal Electricity Generation (t)":   {"factor": 2252.34, "ncv": 25.80,   "unit": "t",  "fuel_type": "Solid",           "is_renewable": False, "source": "IPCC AR6, UK DESNZ 2024", "methodology": "Scope 1 - Stationary Combustion", "scope": "Scope 1",  "category": "Stationary Combustion"},
    "Coal Domestic (t)":                 {"factor": 2883.26, "ncv": 25.80,   "unit": "t",  "fuel_type": "Solid",           "is_renewable": False, "source": "IPCC AR6, UK DESNZ 2024", "methodology": "Scope 1 - Stationary Combustion", "scope": "Scope 1",  "category": "Stationary Combustion"},
    "Coking Coal (t)":                   {"factor": 3165.24, "ncv": 28.20,   "unit": "t",  "fuel_type": "Solid",           "is_renewable": False, "source": "IPCC AR6, UK DESNZ 2024", "methodology": "Scope 1 - Stationary Combustion", "scope": "Scope 1",  "category": "Stationary Combustion"},
    "Petroleum Coke (t)":                {"factor": 3386.86, "ncv": 32.50,   "unit": "t",  "fuel_type": "Solid",           "is_renewable": False, "source": "IPCC AR6, UK DESNZ 2024", "methodology": "Scope 1 - Stationary Combustion", "scope": "Scope 1",  "category": "Stationary Combustion"},
    "Coal Electricity Home Produced (t)":{"factor": 2248.82, "ncv": 25.80,   "unit": "t",  "fuel_type": "Solid",           "is_renewable": False, "source": "IPCC AR6, UK DESNZ 2024", "methodology": "Scope 1 - Stationary Combustion", "scope": "Scope 1",  "category": "Stationary Combustion"},

    # Bioenergy - Liquid Biofuels (L = litres; factors kgCO2e/L; NCV GJ/L)
    "Bioethanol (L)":                    {"factor": 0.00901, "ncv": 0.02120,  "unit": "L",  "fuel_type": "Liquid Biofuel",  "is_renewable": True,  "source": "IPCC AR6, UK DESNZ 2024", "methodology": "Biogenic - Reported Separately", "scope": "Biogenic", "category": "Biogenic Emissions"},
    "Biodiesel ME (L)":                  {"factor": 0.16751, "ncv": 0.03280,  "unit": "L",  "fuel_type": "Liquid Biofuel",  "is_renewable": True,  "source": "IPCC AR6, UK DESNZ 2024", "methodology": "Biogenic - Reported Separately", "scope": "Biogenic", "category": "Biogenic Emissions"},
    "Biodiesel ME Used Cooking Oil (L)": {"factor": 0.16751, "ncv": 0.03280,  "unit": "L",  "fuel_type": "Liquid Biofuel",  "is_renewable": True,  "source": "IPCC AR6, UK DESNZ 2024", "methodology": "Biogenic - Reported Separately", "scope": "Biogenic", "category": "Biogenic Emissions"},
    "Biodiesel ME Tallow (L)":           {"factor": 0.16751, "ncv": 0.03280,  "unit": "L",  "fuel_type": "Liquid Biofuel",  "is_renewable": True,  "source": "IPCC AR6, UK DESNZ 2024", "methodology": "Biogenic - Reported Separately", "scope": "Biogenic", "category": "Biogenic Emissions"},

    # Bioenergy - Solid Biomass (t = metric tonne; factors kgCO2e/t; NCV GJ/t)
    "Wood Logs (t)":                     {"factor": 61.81736,"ncv": 15.60,    "unit": "t",  "fuel_type": "Solid Biomass",   "is_renewable": True,  "source": "IPCC AR6, UK DESNZ 2024", "methodology": "Biogenic - Reported Separately", "scope": "Biogenic", "category": "Biogenic Emissions"},
    "Wood Chips (t)":                    {"factor": 57.15269,"ncv": 11.60,    "unit": "t",  "fuel_type": "Solid Biomass",   "is_renewable": True,  "source": "IPCC AR6, UK DESNZ 2024", "methodology": "Biogenic - Reported Separately", "scope": "Biogenic", "category": "Biogenic Emissions"},
    "Wood Pellets (t)":                  {"factor": 72.61754,"ncv": 17.00,    "unit": "t",  "fuel_type": "Solid Biomass",   "is_renewable": True,  "source": "IPCC AR6, UK DESNZ 2024", "methodology": "Biogenic - Reported Separately", "scope": "Biogenic", "category": "Biogenic Emissions"},
    "Grass Straw (t)":                   {"factor": 49.23656,"ncv": 14.50,    "unit": "t",  "fuel_type": "Solid Biomass",   "is_renewable": True,  "source": "IPCC AR6, UK DESNZ 2024", "methodology": "Biogenic - Reported Separately", "scope": "Biogenic", "category": "Biogenic Emissions"},

    # Bioenergy - Biogas (t = metric tonne; factors kgCO2e/t; NCV GJ/t)
    "Biogas (t)":                        {"factor": 1.21518, "ncv": 17.50,    "unit": "t",  "fuel_type": "Gaseous Biofuel", "is_renewable": True,  "source": "IPCC AR6, UK DESNZ 2024", "methodology": "Biogenic - Reported Separately", "scope": "Biogenic", "category": "Biogenic Emissions"},
    "Landfill Gas (t)":                  {"factor": 0.68793, "ncv": 16.00,    "unit": "t",  "fuel_type": "Gaseous Biofuel", "is_renewable": True,  "source": "IPCC AR6, UK DESNZ 2024", "methodology": "Biogenic - Reported Separately", "scope": "Biogenic", "category": "Biogenic Emissions"},

    # Additional Refrigerants (GWP from IPCC AR5)
    "SF6 (kg)":      {"factor": 22800.0, "ncv": 0.0, "unit": "kg", "fuel_type": "Refrigerant", "is_renewable": False, "source": "IPCC AR5", "methodology": "Scope 1 - Fugitive Emissions", "scope": "Scope 1", "category": "Refrigerants"},
    "HFC-23 (kg)":   {"factor": 14800.0, "ncv": 0.0, "unit": "kg", "fuel_type": "Refrigerant", "is_renewable": False, "source": "IPCC AR5", "methodology": "Scope 1 - Fugitive Emissions", "scope": "Scope 1", "category": "Refrigerants"},
    "HFC-125 (kg)":  {"factor": 3500.0,  "ncv": 0.0, "unit": "kg", "fuel_type": "Refrigerant", "is_renewable": False, "source": "IPCC AR5", "methodology": "Scope 1 - Fugitive Emissions", "scope": "Scope 1", "category": "Refrigerants"},
    "HFC-143a (kg)": {"factor": 4470.0,  "ncv": 0.0, "unit": "kg", "fuel_type": "Refrigerant", "is_renewable": False, "source": "IPCC AR5", "methodology": "Scope 1 - Fugitive Emissions", "scope": "Scope 1", "category": "Refrigerants"},
    "HFC-227ea (kg)":{"factor": 3220.0,  "ncv": 0.0, "unit": "kg", "fuel_type": "Refrigerant", "is_renewable": False, "source": "IPCC AR5", "methodology": "Scope 1 - Fugitive Emissions", "scope": "Scope 1", "category": "Refrigerants"},
    "HFC-236fa (kg)":{"factor": 9810.0,  "ncv": 0.0, "unit": "kg", "fuel_type": "Refrigerant", "is_renewable": False, "source": "IPCC AR5", "methodology": "Scope 1 - Fugitive Emissions", "scope": "Scope 1", "category": "Refrigerants"},
    "R-507A (kg)":   {"factor": 3985.0,  "ncv": 0.0, "unit": "kg", "fuel_type": "Refrigerant", "is_renewable": False, "source": "IPCC AR5", "methodology": "Scope 1 - Fugitive Emissions", "scope": "Scope 1", "category": "Refrigerants"},
    "R-508B (kg)":   {"factor": 13396.0, "ncv": 0.0, "unit": "kg", "fuel_type": "Refrigerant", "is_renewable": False, "source": "IPCC AR5", "methodology": "Scope 1 - Fugitive Emissions", "scope": "Scope 1", "category": "Refrigerants"},
    "R-407F (kg)":   {"factor": 1825.0,  "ncv": 0.0, "unit": "kg", "fuel_type": "Refrigerant", "is_renewable": False, "source": "IPCC AR5", "methodology": "Scope 1 - Fugitive Emissions", "scope": "Scope 1", "category": "Refrigerants"},
    "R-408A (kg)":   {"factor": 3152.0,  "ncv": 0.0, "unit": "kg", "fuel_type": "Refrigerant", "is_renewable": False, "source": "IPCC AR5", "methodology": "Scope 1 - Fugitive Emissions", "scope": "Scope 1", "category": "Refrigerants"},
}

FUEL_MAPPING = {
    "electricity": "Grid Electricity (kWh)", "kwh": "Grid Electricity (kWh)", "power": "Grid Electricity (kWh)", "nre": "Grid Electricity (kWh)",
    "solar": "Solar (kWh)", "wind": "Wind (kWh)", "hydel": "Hydel (kWh)", "hydro": "Hydel (kWh)", "hydroelectric": "Hydel (kWh)",
    "re": "Renewable Electricity (kWh)", "renewable": "Renewable Electricity (kWh)", "ppa": "Renewable Electricity (kWh)", "rooftop": "Renewable Electricity (kWh)", "open access": "Renewable Electricity (kWh)",
    "hsd": "HSD (KL)", "diesel": "HSD (KL)", "natural gas": "Natural Gas (SCM)", "ng": "Natural Gas (SCM)",
    "lpg": "LPG (Kg)", "fo": "Furnace Oil (KL)", "furnace oil": "Furnace Oil (KL)", "furnance oil": "Furnace Oil (KL)",
    "ldo": "LDO (KL)", "lshs": "LSHS (KL)", "petrol": "Petrol (KL)",
    "biofuel": "Biofuel (KL)", "biodiesel": "Biodiesel (KL)", "briquettes": "Briquettes (Kg)",
    "r152a": "R152a (kg)", "r-152a": "R152a (kg)",
    "r22": "R22 (kg)", "r134a": "R134a (kg)", "r-134a": "R134a (kg)", "r134": "R134a (kg)",
    "r407c": "R-407C (kg)", "r-407c": "R-407C (kg)", "r407": "R-407C (kg)",
    "r404a": "R-404A (kg)", "r-404a": "R-404A (kg)", "r404": "R-404A (kg)",
    "r410a": "R-410A (kg)", "r-410a": "R-410A (kg)", "r410": "R-410A (kg)",
    "r407a": "R-407A (kg)", "r-407a": "R-407A (kg)",
    "r123": "R123 (kg)", "r-123": "R123 (kg)",
    "r32": "R32 (kg)", "r-32": "R32 (kg)", 
    "hsd (mobile)": "HSD Mobile (KL)",
    "hsd mobile": "HSD Mobile (KL)",
    # Additional fuels (IPCC AR6 / UK DESNZ)
    "cng": "CNG (L)", "compressed natural gas": "CNG (L)",
    "lng": "LNG (L)", "liquefied natural gas": "LNG (L)",
    "lpg litre": "LPG (L)", "lpg l": "LPG (L)",
    "aviation spirit": "Aviation Spirit (L)", "avgas": "Aviation Spirit (L)",
    "aviation turbine": "Aviation Turbine Fuel (L)", "avtur": "Aviation Turbine Fuel (L)", "jet fuel": "Aviation Turbine Fuel (L)", "atf": "Aviation Turbine Fuel (L)",
    "burning oil": "Burning Oil (L)", "kerosene": "Burning Oil (L)",
    "diesel biofuel blend": "Diesel Average Biofuel Blend (L)",
    "diesel mineral": "Diesel 100% Mineral (L)",
    "fuel oil": "Fuel Oil (L)",
    "gas oil": "Gas Oil (L)",
    "lubricant": "Lubricants (L)",
    "naphtha": "Naphtha (L)",
    "petrol biofuel blend": "Petrol Average Biofuel Blend (L)",
    "petrol mineral": "Petrol 100% Mineral (L)",
    "marine gas oil": "Marine Gas Oil (L)", "mgo": "Marine Gas Oil (L)",
    "marine fuel oil": "Marine Fuel Oil (L)", "hfo": "Marine Fuel Oil (L)",
    "waste oil": "Waste Oils (L)",
    "other petroleum gas": "Other Petroleum Gas (L)",
    "coal industrial": "Coal Industrial (t)",
    "coal electricity": "Coal Electricity Generation (t)",
    "coal domestic": "Coal Domestic (t)",
    "coking coal": "Coking Coal (t)",
    "petroleum coke": "Petroleum Coke (t)",
    "bioethanol": "Bioethanol (L)", "ethanol": "Bioethanol (L)",
    "biodiesel me used cooking oil": "Biodiesel ME Used Cooking Oil (L)",
    "biodiesel me tallow": "Biodiesel ME Tallow (L)",
    "biodiesel me": "Biodiesel ME (L)",
    "wood log": "Wood Logs (t)", "wood chip": "Wood Chips (t)",
    "wood pellet": "Wood Pellets (t)",
    "grass straw": "Grass Straw (t)", "grass": "Grass Straw (t)", "straw": "Grass Straw (t)",
    "biogas": "Biogas (t)", "landfill gas": "Landfill Gas (t)",
    "sf6": "SF6 (kg)", "sulphur hexafluoride": "SF6 (kg)",
    "hfc-23": "HFC-23 (kg)", "hfc23": "HFC-23 (kg)",
    "hfc-125": "HFC-125 (kg)", "hfc125": "HFC-125 (kg)",
    "hfc-143a": "HFC-143a (kg)", "hfc143a": "HFC-143a (kg)",
    "hfc-227ea": "HFC-227ea (kg)", "r507a": "R-507A (kg)", "r-507a": "R-507A (kg)",
    "r508b": "R-508B (kg)", "r-508b": "R-508B (kg)",
    "r407f": "R-407F (kg)", "r-407f": "R-407F (kg)",
    "r408a": "R-408A (kg)", "r-408a": "R-408A (kg)",
    "fire extinguisher refilled: 1 kg": "Fire Extinguisher 1kg (nos)",
    "fire extinguisher refilled: 2 kg": "Fire Extinguisher 2kg (nos)",
    "fire extinguisher refilled: 2.5 kg": "Fire Extinguisher 2.5kg (nos)",
    "fire extinguisher refilled: 3 kg": "Fire Extinguisher 3kg (nos)",
    "fire extinguisher refilled: 3.5 kg": "Fire Extinguisher 3.5kg (nos)",
    "fire extinguisher refilled: 4.5 kg": "Fire Extinguisher 4.5kg (nos)",
    "fire extinguisher refilled: 6 kg": "Fire Extinguisher 6kg (nos)",
    "fire extinguisher refilled: 9 kg": "Fire Extinguisher 9kg (nos)",
    "fire extinguisher refilled: 22.5 kg": "Fire Extinguisher 22.5kg (nos)"
}

INDIAN_CITIES = ['nashik', 'baddi', 'goa', 'indore', 'sikkim', 'mumbai', 'delhi', 'bangalore', 'chennai', 'hyderabad', 'pune', 'gurgaon', 'noida', 'ahmedabad', 'kolkata', 'nalagarh', 'dindori', 'aurangabad', 'mahape', 'sinnar', 'taloja', 'sanpada', 'mohol']
EXCLUDE_KW = ["total", "sum", "emission factor", "emissions tco2", "tco2e", "tco2", "ncv", "intensity", "source", "methodology", "scope", "category", "density", "gwp", "parameter", "notes", "co2e emission"]
RENEWABLE_FUEL_TYPES = {"Biofuel (KL)", "Biodiesel (KL)", "Briquettes (Kg)"}
EXPECTED_BASELINE_EF = {"Furnace Oil (KL)": 3010.0, "Natural Gas (SCM)": 2.15}
LOW_QUANTITY_WARNINGS = {"HSD (KL)": 1.0, "HSD Mobile (KL)": 1.0}
EF_ABS_TOLERANCE = 0.01

# --- Intelligence Engine ---

FUEL_KEYS_SORTED = sorted(FUEL_MAPPING.keys(), key=len, reverse=True)

def map_fuel_name(text, default="Unknown"):
    """Maps a free-text fuel label to the closest canonical fuel in FUEL_MAPPING."""
    t = str(text).strip().lower()
    if not t:
        return default

    # Priority for renewable classification to avoid generic "kWh/power" taking precedence.
    if re.search(r"(?<![a-z0-9])nre(?![a-z0-9])", t):
        return "Grid Electricity (kWh)"
    if re.search(r"(?<![a-z0-9])(hydel|hydro|hydroelectric)(?![a-z0-9])", t):
        return "Hydel (kWh)"
    if re.search(r"(?<![a-z0-9])solar(?![a-z0-9])", t):
        return "Solar (kWh)"
    if re.search(r"(?<![a-z0-9])wind(?![a-z0-9])", t):
        return "Wind (kWh)"
    if re.search(r"(?<![a-z0-9])(re|renewable|open access|rooftop|ppa)(?![a-z0-9])", t):
        return "Renewable Electricity (kWh)"

    # Strict token matching first to avoid false positives (e.g., "re" inside unrelated words).
    for kw in FUEL_KEYS_SORTED:
        pattern = rf"(?<![a-z0-9]){re.escape(kw)}(?![a-z0-9])"
        if re.search(pattern, t):
            return FUEL_MAPPING[kw]

    # Fallback contains-match for messy headers that include punctuation/merged tokens.
    for kw in FUEL_KEYS_SORTED:
        if kw in t:
            return FUEL_MAPPING[kw]
    return default

def is_activity_location(loc):
    l = str(loc).strip().lower()
    if l in ["", "nan", "none", "null", "n/a", "-"]:
        return False
    bad_tokens = ["parameter", "source", "density", "ncv", "gwp", "co2", "ch4", "n2o", "emission factor", "notes", "site"]
    if any(tok in l for tok in bad_tokens):
        return False
    return True

def is_reference_text(text):
    t = str(text).lower()
    ref_tokens = [
        "emission factors", "parameter", "density", "ncv", "gwp",
        "co2 ef", "ch4 ef", "n2o ef", "source", "defra", "ipcc", "tco2e", "co2e emissions"
    ]
    return any(tok in t for tok in ref_tokens)

def is_valid_reporting_period(period_text):
    p = str(period_text).strip().upper()
    if not p:
        return False
    if "FY " in p and re.search(r"20\d{2}\s*[-/]\s*\d{2,4}", p):
        return True
    if re.search(r"\b20\d{2}\s*[-/]\s*\d{2,4}\b", p):
        return True
    return False

def build_validation_notes(f_type, period, scope, quantity, adjusted_ef, tco2e):
    notes = []
    expected_ef = EXPECTED_BASELINE_EF.get(f_type)
    if expected_ef is not None and abs(adjusted_ef - expected_ef) > EF_ABS_TOLERANCE:
        notes.append(f"EF mismatch: expected ~{expected_ef:g}, got {adjusted_ef:.4f}")
    min_qty = LOW_QUANTITY_WARNINGS.get(f_type)
    if min_qty is not None and 0 < quantity < min_qty:
        notes.append(
            f"Low quantity warning: {f_type} quantity {quantity:.4f} < {min_qty:.1f} "
            "may be pre-calculated emissions input"
        )
    if f_type in RENEWABLE_FUEL_TYPES and scope != "Biogenic":
        notes.append("Scope mismatch: biogenic fuel should be tagged as Biogenic")
    if not is_valid_reporting_period(period):
        notes.append("Period format warning: expected FY YYYY-YY")
    if tco2e > 5000:
        notes.append("High emission outlier: row exceeds 5,000 tCO2e")
    return "; ".join(notes) if notes else "OK"

def extract_custom_ef_from_header(text):
    """Extract custom EF from header text and normalize to kgCO2e/kWh when unit is explicit."""
    t = str(text).lower()
    if "custom" not in t:
        return None

    patterns = [
        r"(?:ef|emission\s*factor)\s*[:=]?\s*(-?\d+(?:\.\d+)?)",
        r"(-?\d+(?:\.\d+)?)\s*(?:kg\s*co2e\s*/\s*kwh|kgco2e\s*/\s*kwh|co2e\s*/\s*kwh)",
        r"(-?\d+(?:\.\d+)?)\s*(?:t\s*co2e\s*/\s*mwh|tco2e\s*/\s*mwh)",
        r"(-?\d+(?:\.\d+)?)\s*(?:g\s*co2e\s*/\s*kwh|gco2e\s*/\s*kwh)"
    ]
    for pat in patterns:
        m = re.search(pat, t)
        if m:
            val = safe_float(m.group(1))
            if "gco2e" in pat or "g\\s*co2e" in pat:
                return val / 1000.0
            return val
    return None

def resolve_fuel_profile(raw_text, mapped_fuel):
    """Resolve final fuel label and optional EF override from raw header text."""
    t = str(raw_text).lower()
    f = mapped_fuel

    is_re_context = any(k in t for k in ["re", "renewable", "solar", "wind", "hydel", "hydro", "green power", "open access", "rooftop", "ppa"]) or ("custom" in t and "kwh" in t)
    custom_ef = extract_custom_ef_from_header(raw_text)
    if custom_ef is not None and is_re_context:
        return (
            "Custom Renewable Electricity (kWh)",
            {
                "factor": custom_ef,
                "ncv": 0.0036,
                "unit": "kWh",
                "is_renewable": True,
                "source": "Custom EF from header",
                "methodology": "Scope 2 - Custom Renewable",
                "scope": "Scope 2",
                "category": "Renewable Energy"
            }
        )

    return f, None

def find_unit(text):
    text_lower = str(text).lower()
    mapping = {
        r'\bkg\b|\bkilogram\b': 'kg',
        r'\bkl\b|\bkilolitre\b|\bkiloliter\b': 'KL',
        r'\bltr\b|\blitre\b|\bliter\b': 'Litre',
        r'\bsch?m\b|\bscm\b': 'SCM',
        r'\bkwh\b|\bunit\b': 'kWh',
        r'\bmt\b|\bmetric tonn?e\b': 'MT'
    }
    for pattern, unit in mapping.items():
        if re.search(pattern, text_lower): return unit
    return "N/A"

def extract_period_metadata(text):
    t = str(text).lower()
    fy_match = re.search(r"(?:fy)?\s*(20\d{2}[\-\/]\d{2,4}|\d{2}[\-\/]\d{2})", t)
    if fy_match: return f"FY {fy_match.group(1)}".upper()
    month_match = re.search(r"([a-z]{3,9})[\s\-\/]*(\d{2,4})", t)
    if month_match:
        m_str, y_str = month_match.groups()
        try:
            m_name = datetime.strptime(m_str[:3], "%b").strftime("%B")
            y_name = f"20{y_str}" if len(y_str) == 2 else y_str
            return f"{m_name} {y_name}"
        except: pass
    year_match = re.search(r"\b(20\d{2})\b", t)
    if year_match: return year_match.group(1)
    return ""

def validate_location(loc):
    if not loc or pd.isna(loc): return "Unknown Site"
    l_str = str(loc).lower().strip()
    if l_str in ['nan', 'none', 'null', 'unknown', 'n/a', '', '-']: return "Unknown Site"
    if any(x == l_str for x in EXCLUDE_KW): return "Unknown Site"
    return str(loc).strip()

def detect_site_type(name):
    name_low = str(name).lower()
    if any(x in name_low for x in ["plant", "factory", "mfg", "unit", "work", "prod"]): return "Manufacturing Site"
    if any(x in name_low for x in ["warehouse", "depot", "wh", "store"]): return "Warehouse"
    if any(x in name_low for x in ["office", "hq", "corp", "tower", "suite"]): return "Office"
    if any(x in name_low for x in ["village", "rural", "gram"]): return "Village/Rural Site"
    return "Commercial Site"

def is_emissions_or_total_column(text):
    t = str(text).strip().lower()
    if not t:
        return False
    return bool(re.search(
        r"tco2e|tco2|kgco2e|co.?2e|emission|total emissions|scope\s*[12]\b",
        t,
        re.IGNORECASE
    ))

def should_skip_sheet(sheet_context):
    s = str(sheet_context or "").strip().lower()
    if not s:
        return False
    return bool(re.search(r"\bconsolidated\b", s))

def detect_and_melt_matrix(df):
    loc_col = None
    for col in df.columns:
        if any(city in df[col].astype(str).str.lower().tolist() for city in INDIAN_CITIES):
            loc_col = col; break
    if not loc_col: return df, False, None
    
    fuel_headers = [
        c for c in df.columns
        if c != loc_col
        and map_fuel_name(c, default=None) is not None
        and not is_reference_text(c)
        and not is_emissions_or_total_column(c)
    ]
    if any(extract_period_metadata(c) for c in fuel_headers):
        fuel_headers = [c for c in fuel_headers if extract_period_metadata(c)]
    if len(fuel_headers) < 2: return df, False, None
    
    # Strictly exclude technical columns
    value_vars = [
        c for c in fuel_headers
        if not any(kw in str(c).lower() for kw in EXCLUDE_KW)
        and not is_emissions_or_total_column(c)
    ]
    
    id_vars = [loc_col]
    for c in df.columns:
        if c != loc_col and any(kw in str(c).lower() for kw in ["month", "year", "fy", "period"]) and c not in value_vars:
            id_vars.append(c)
            
    melted = pd.melt(df, id_vars=id_vars, value_vars=value_vars, var_name='Fuel / Electricity Type', value_name='Quantity')
    melted = melted.dropna(subset=['Quantity'])
    melted['Quantity'] = melted['Quantity'].apply(safe_float)
    return melted[melted['Quantity'] > 0], True, loc_col

def split_sheet_into_tables(df):
    """Splits a sheet into multiple DataFrames based on 2+ consecutive empty rows."""
    mask = df.isna().all(axis=1)
    # Only split on gaps of 2+ consecutive empty rows (single empty rows are common between headers and data)
    is_gap = mask & mask.shift(1, fill_value=False)
    groups = is_gap.cumsum()
    sub_dfs = [group_df for _, group_df in df[~mask].groupby(groups[~mask]) if len(group_df) > 1]
    return sub_dfs if sub_dfs else ([df[~mask]] if len(df[~mask]) > 1 else [])

def _classify_electricity_header_token(value):
    text = str(value).strip().lower()
    if not text or text in {"nan", "none"}:
        return None
    compact = re.sub(r"\s+", " ", text)
    if "kwh" in compact:
        return "kwh"
    if re.search(r"(?<![a-z0-9])(nre|non[-\s]*renewable)(?![a-z0-9])", compact):
        return "nre"
    if re.search(r"(?<![a-z0-9])(re|renewable|solar|wind|hydel|hydro|ppa|open access|rooftop)(?![a-z0-9])", compact):
        return "re"
    return None


def process_electricity_sheet(raw_df, default_period=""):
    """Special parser for electricity sheets containing multi-year NRE/RE blocks."""
    rows = []
    df = raw_df.copy()
    location_fallback_col = 0

    def _is_valid_site_label(site_text):
        s = str(site_text).strip().lower()
        if s in ["", "nan", "none", "null", "n/a", "-"]:
            return False
        bad_tokens = [
            "total", "energy gj", "energy renewable", "energy non renewable",
            "emissions", "re %", "scope 2 -", "tco2", "intensity"
        ]
        if any(tok in s for tok in bad_tokens):
            return False
        return True

    for r in range(len(df)):
        raw_row_vals = df.iloc[r].tolist()
        token_labels = [_classify_electricity_header_token(v) for v in raw_row_vals]
        has_nre = "nre" in token_labels
        has_re = "re" in token_labels
        if not (has_nre and has_re):
            continue

        # Find year row above descriptor row (typically immediately above).
        year_row_idx = max(r - 1, 0)
        year_vals = [str(v) if pd.notna(v) else "" for v in df.iloc[year_row_idx].tolist()]

        # Detect repeated groups where NRE/RE appear.
        for c in range(len(token_labels)):
            token = token_labels[c]
            if token not in ["kwh", "nre"]:
                continue
            if token == "nre" and c - 1 >= 0 and token_labels[c - 1] == "kwh":
                continue

            if token == "kwh":
                nre_col = c + 1 if c + 1 < len(token_labels) and token_labels[c + 1] == "nre" else None
                re_col = c + 2 if c + 2 < len(token_labels) and token_labels[c + 2] == "re" else None
                site_col = c - 1
            else:
                nre_col = c
                re_col = c + 1 if c + 1 < len(token_labels) and token_labels[c + 1] == "re" else None
                site_col = c - 2 if c - 1 >= 0 and token_labels[c - 1] == "kwh" else c - 1

            if nre_col is None:
                continue
            re_fuel_type = (
                map_fuel_name(raw_row_vals[re_col], default="Renewable Electricity (kWh)")
                if re_col is not None else "Renewable Electricity (kWh)"
            )

            period = ""
            for cc in [nre_col, nre_col - 1, nre_col + 1, nre_col - 2, nre_col + 2]:
                if 0 <= cc < len(year_vals) and not period:
                    period = extract_period_metadata(year_vals[cc]) or period
            period = period or default_period

            # Read data rows until we hit next section.
            for rr in range(r + 1, len(df)):
                first_txt = str(df.iloc[rr, 0]).strip().lower() if pd.notna(df.iloc[rr, 0]) else ""
                if first_txt in ["", "nan"]:
                    continue
                if any(k in first_txt for k in ["total", "energy gj", "emissions", "re %", "scope 2 -"]):
                    break

                site_candidate = (
                    df.iloc[rr, site_col]
                    if 0 <= site_col < df.shape[1] and pd.notna(df.iloc[rr, site_col])
                    else None
                )
                candidate_text = str(site_candidate).strip()
                site_val = (
                    site_candidate
                    if is_activity_location(site_candidate) and re.search(r"[A-Za-z]", candidate_text)
                    else df.iloc[rr, location_fallback_col]
                )
                site = str(site_val).strip()
                if not is_activity_location(site) or not _is_valid_site_label(site):
                    continue

                nre_qty = safe_float(df.iloc[rr, nre_col]) if 0 <= nre_col < df.shape[1] else 0.0
                re_qty = safe_float(df.iloc[rr, re_col]) if re_col is not None and 0 <= re_col < df.shape[1] else 0.0

                if nre_qty > 0 and is_valid_reporting_period(period):
                    rows.append(process_standard_row("Grid Electricity (kWh)", nre_qty, period, site, "kWh"))
                if re_qty > 0 and is_valid_reporting_period(period):
                    rows.append(process_standard_row(re_fuel_type, re_qty, period, site, "kWh"))

    # Deduplicate rows created from overlapping group detection.
    if not rows:
        return rows
    seen = set()
    dedup = []
    for row in rows:
        key = (row["Period"], row["Location"], row["Fuel / Electricity Type"], round(row["Quantity"], 6))
        if key in seen:
            continue
        seen.add(key)
        dedup.append(row)
    return dedup

def process_table_block(df, parent_period, sheet_context=""):
    """Processes a single block of data (matrix or standard)."""
    if should_skip_sheet(sheet_context):
        return []

    is_mobile = "mobile" in sheet_context

    # Trim trailing calculation/reference sections that are commonly embedded in the same sheet
    # (e.g., "CO2e Emissions", "Emission Factors", notes blocks).
    stop_re = re.compile(r"co.?2e emissions|emission factors|total scope 1 emissions|notes:", re.IGNORECASE)
    cutoff = None
    for idx in range(len(df)):
        preview = " ".join(str(x) for x in df.iloc[idx, :6].tolist() if pd.notna(x)).lower()
        if idx > 5 and stop_re.search(preview):
            cutoff = idx
            break
    if cutoff is not None:
        df = df.iloc[:cutoff].reset_index(drop=True)

    # 1. Strip top empty rows/trash
    df = df.dropna(how='all', axis=1)
    if df.empty:
        return []

    top_blob = " ".join(
        str(v) for v in df.head(3).fillna("").values.flatten().tolist() if str(v).strip()
    ).lower()
    if re.search(r"co.?2e emissions|emission factors|scope 1 consolidated ghg emissions", top_blob):
        return []
    
    # 2. Find header row (use word-boundary matching to avoid short-keyword false positives)
    header_keywords = [kw for kw in FUEL_MAPPING.keys() if len(kw) >= 3] + ["date", "period", "loc", "site", "year", "month", "qty", "unit", "location", "quantity", "consumption"]
    header_patterns = [re.compile(rf'\b{re.escape(kw)}\b', re.IGNORECASE) for kw in header_keywords]
    best_row, max_score = 0, -1
    for idx in range(min(10, len(df))):
        row = df.iloc[idx].astype(str).str.lower()
        score = sum(1 for cell in row if any(p.search(str(cell)) for p in header_patterns))
        if score > max_score: max_score, best_row = score, idx
    
    if max_score > 0:
        new_cols = df.iloc[best_row].tolist()
        # Check rows above for FY period metadata (multi-row headers)
        # Build per-column period context from the row above
        period_row = [None] * len(new_cols)
        for prev_idx in range(best_row):
            prev_vals = df.iloc[prev_idx].tolist()
            last_period = None
            for ci, cell in enumerate(prev_vals):
                p = extract_period_metadata(str(cell)) if pd.notna(cell) else None
                if p:
                    last_period = p
                if last_period:
                    period_row[ci] = last_period
        # Set parent_period from first detected period if not already set
        if not parent_period:
            parent_period = next((p for p in period_row if p), "")

        # Build clean column names, prepending period for multi-year columns
        clean_cols = []
        has_multi_periods = len(set(p for p in period_row if p)) > 1
        for i, c in enumerate(new_cols):
            col_name = str(c).strip() if pd.notna(c) else f"_col_{i}"
            if has_multi_periods and period_row[i]:
                col_name = f"{period_row[i]} | {col_name}"
            clean_cols.append(col_name)

        # Deduplicate remaining duplicates
        seen = {}
        for i, c in enumerate(clean_cols):
            if c in seen:
                seen[c] += 1
                clean_cols[i] = f"{c}_{seen[c]}"
            else:
                seen[c] = 0
        df.columns = clean_cols
        df = df.iloc[best_row+1:].reset_index(drop=True)
    
    # Filter out total/sum/subtotal rows
    def _is_total_row(r):
        try:
            return r.astype(str).str.contains(r'total|sum|grand|subtotal|emission.?factor|emissions?\s*tco2|tco2e|density|gwp|parameter|co2e.?emission|notes:|ipcc|ghg protocol', case=False, na=False).any()
        except Exception:
            return False
    df = df[~df.apply(_is_total_row, axis=1)]
    if df.empty:
        return []

    # Drop columns that are emissions/summary outputs to avoid re-ingestion as activity data.
    drop_cols = []
    for c in df.columns:
        c_low = str(c).lower()
        if is_emissions_or_total_column(c_low):
            if any(k in c_low for k in ["site", "location", "plant", "branch", "area", "month", "year", "fy", "period"]):
                continue
            drop_cols.append(c)
    if drop_cols:
        df = df.drop(columns=drop_cols, errors="ignore")
    if df.empty:
        return []
    
    # 3. Detect Matrix
    m_df, is_matrix, loc_col = detect_and_melt_matrix(df)
    
    # Remap fuel types for mobile combustion context
    def _resolve_fuel(f_type):
        if is_mobile and f_type in ["HSD (KL)", "HSD (Mobile)"]:
            return "HSD Mobile (KL)"
        return f_type

    results = []
    if is_matrix:
        # Directly process melted rows
        for _, r in m_df.iterrows():
            loc = str(r[loc_col])
            if not is_activity_location(loc):
                continue
            f_t_raw = str(r['Fuel / Electricity Type'])
            f_type = map_fuel_name(f_t_raw, default="Unknown")
            f_type = _resolve_fuel(f_type)
            f_type, ef_override = resolve_fuel_profile(f_t_raw, f_type)
            val = safe_float(r['Quantity'])

            # Period detection (prioritize fuel header / value column metadata, then row, then parent)
            row_p = extract_period_metadata(f_t_raw) or parent_period
            period_cols = [c for c in r.index if any(kw in str(c).lower() for kw in ["month", "year", "fy", "period"]) and c != 'Fuel / Electricity Type']
            if period_cols and pd.notna(r[period_cols[0]]): row_p = extract_period_metadata(r[period_cols[0]]) or row_p

            if val > 0 and is_valid_reporting_period(row_p):
                results.append(process_standard_row(f_type, val, row_p, loc, find_unit(f_t_raw), ef_override))
    else:
        # Standard processing
        cl, df = classify_headers(df)
        for _, r in df.iterrows():
            row_p = parent_period
            if cl["period"] and pd.notna(r[cl["period"]]): row_p = extract_period_metadata(r[cl["period"]]) or row_p
            loc = str(r[cl['location'][0]]) if cl['location'] else "Unknown Site"
            if not is_activity_location(loc):
                continue

            for c, f_t in cl["fuels"].items():
                val = safe_float(r[c])
                if val > 0:
                    cur_p = extract_period_metadata(c) or row_p if f_t == "Date-Mapped Quantity" else row_p
                    if is_valid_reporting_period(cur_p):
                        resolved_fuel = _resolve_fuel(f_t)
                        resolved_fuel, ef_override = resolve_fuel_profile(c, resolved_fuel)
                        results.append(process_standard_row(resolved_fuel, val, cur_p, loc, find_unit(c), ef_override))
            if not cl["fuels"]:
                for q in cl["qty"]:
                    val = safe_float(r[q['name']])
                    if val > 0:
                        if is_valid_reporting_period(row_p):
                            resolved_fuel = _resolve_fuel(q['fuel'] or "Unknown")
                            resolved_fuel, ef_override = resolve_fuel_profile(q['name'], resolved_fuel)
                            results.append(process_standard_row(resolved_fuel, val, row_p, loc, find_unit(q['name']), ef_override))
    
    return results

def remove_double_ingested_rows(rdf, rel_tol=0.02):
    """Drop rows that look like pre-calculated tCO2e re-ingested as Quantity."""
    if rdf is None or rdf.empty:
        return rdf, 0
    df = rdf.copy().reset_index(drop=True)
    suspect_idx = set()
    keys = ["Period", "Location", "Fuel / Electricity Type"]
    for _, grp in df.groupby(keys, dropna=False):
        if len(grp) < 2:
            continue
        recs = grp[["Quantity", "Total Emissions (tCO2e)"]].to_dict(orient="records")
        gidx = list(grp.index)
        for i, ri in enumerate(recs):
            qi = safe_float(ri.get("Quantity", 0))
            ti = safe_float(ri.get("Total Emissions (tCO2e)", 0))
            for j, rj in enumerate(recs):
                if i == j:
                    continue
                tj = safe_float(rj.get("Total Emissions (tCO2e)", 0))
                if tj <= 0:
                    continue
                if abs(qi - tj) <= max(0.01, rel_tol * tj):
                    if ti > (tj * 1.2):
                        suspect_idx.add(gidx[i])
                        break
    if not suspect_idx:
        return df, 0
    cleaned = df.drop(index=sorted(suspect_idx)).reset_index(drop=True)
    return cleaned, len(suspect_idx)

def classify_headers(df):
    cl = {"location": [], "period": None, "fuels": {}, "qty": [], "other": []}
    sample_blob = " ".join(df.astype(str).head(10).values.flatten().tolist()).lower()
    guessed_fuel = map_fuel_name(sample_blob, default=None)

    for col in df.columns:
        c_low = str(col).lower()
        if any(kw in c_low for kw in EXCLUDE_KW): continue
        if is_emissions_or_total_column(c_low): continue
        if is_reference_text(col): continue
        is_num = pd.api.types.is_numeric_dtype(df[col])
        p_ext = extract_period_metadata(col)
        fuel = map_fuel_name(c_low, default=None)

        # If a column carries both period and fuel semantics (e.g., "FY 2024-25 | Petrol"),
        # keep the fuel mapping so each value can be attributed correctly.
        if p_ext and is_num and fuel:
            cl["fuels"][col] = fuel
            continue
        
        if p_ext:
            if is_num: cl["fuels"][col] = "Date-Mapped Quantity"
            else: cl["period"] = col
            continue
        if any(kw in c_low for kw in ["month", "year", "fy", "period"]) and not "qty" in c_low: cl["period"] = col; continue
        if fuel: cl["fuels"][col] = fuel; continue
        if any(x in c_low for x in ["site", "location", "plant", "branch", "area"]): cl["location"].append(col); continue
        if is_num: cl["qty"].append({"name": col, "fuel": guessed_fuel})

    if any(extract_period_metadata(c) for c in cl["fuels"].keys()):
        cl["fuels"] = {c: f for c, f in cl["fuels"].items() if extract_period_metadata(c)}
    return cl, df

def process_standard_row(f_type, qty, period="", loc="Unknown", u_in="N/A", ef_override=None):
    q_val = safe_float(qty)
    ef = EF_DATABASE.get(f_type, {"factor": 0, "ncv": 0, "unit": "N/A", "scope": "N/A", "category": "N/A", "is_renewable": False}).copy()
    if ef_override:
        ef.update(ef_override)
    adj = 0.001 if (u_in.lower() in ["litre", "ltr", "liter"] and ef['unit'] == "KL") else 1.0
    adj_ef = ef['factor'] * adj
    kg_co2 = q_val * adj_ef
    t_co2 = kg_co2 / 1000
    validation_notes = build_validation_notes(
        f_type=f_type,
        period=period,
        scope=ef.get("scope", "N/A"),
        quantity=q_val,
        adjusted_ef=adj_ef,
        tco2e=t_co2,
    )
    return {
        "Period": period, "Location": validate_location(loc), "Site Type": detect_site_type(loc),
        "Scope": ef.get('scope', 'N/A'), "Category": ef.get('category', 'N/A'), "Fuel / Electricity Type": f_type,
        "Fuel Type": ef.get('fuel_type', 'N/A'),
        "Quantity": q_val, "Unit": u_in if u_in != "N/A" else ef.get('unit', 'N/A'),
        "EF Original Unit": ef.get('unit', 'N/A'), "Unit Adjusted EF": adj_ef,
        "Energy Usage (GJ)": q_val * adj * ef['ncv'], "Total Emissions (kgCO2e)": kg_co2,
        "Total Emissions (tCO2e)": t_co2,
        "Factor Source": ef.get('source', 'N/A'), "Methodology": ef.get('methodology', 'N/A'),
        "Validation Notes": validation_notes
    }

def safe_float(v):
    if pd.isna(v) or v is None: return 0.0
    clean_v = re.sub(r'[^\d\.-]', '', str(v).strip())
    try: return float(clean_v) if clean_v else 0.0
    except: return 0.0

def energy_gj_from_row(row):
    """Compute energy in GJ from quantity+unit (not emissions), with fuel-aware conversion fallback."""
    qty = safe_float(row.get("Quantity", 0))
    f_type = str(row.get("Fuel / Electricity Type", ""))
    unit = str(row.get("Unit", "")).strip().lower()
    ef = EF_DATABASE.get(f_type, {"unit": "N/A", "ncv": 0.0})
    ef_unit = str(ef.get("unit", "")).strip().lower()
    ncv = safe_float(ef.get("ncv", 0.0))

    if unit in ["gj"]:
        return qty
    if unit in ["mwh"]:
        return qty * 3.6
    if unit in ["kwh", "unit"]:
        return qty * 0.0036
    if unit in ["l", "ltr", "litre", "liter"] and ef_unit == "l":
        return qty * ncv
    if unit in ["l", "ltr", "litre", "liter"] and ef_unit == "kl":
        return qty * 0.001 * ncv
    if unit == "kl" and ef_unit == "kl":
        return qty * ncv
    if unit in ["kg", "kilogram"] and ef_unit == "kg":
        return qty * ncv
    if unit == "scm" and ef_unit == "scm":
        return qty * ncv
    if unit in ["m3", "m\u00b3", "cubic metres", "cubic meter", "nm3"] and ef_unit == "m\u00b3":
        return qty * ncv
    if unit in ["t", "tonne", "tonnes", "mt", "ton", "tons"] and ef_unit == "t":
        return qty * ncv

    # Fallback to precomputed value if unit mapping is unclear.
    return safe_float(row.get("Energy Usage (GJ)", 0.0))

def get_fy_start(period_text):
    m = re.search(r"FY\s*(20\d{2})\s*[-/]\s*(\d{2,4})", str(period_text), re.IGNORECASE)
    if not m:
        return None
    return int(m.group(1))

def format_fy(start_year):
    return f"FY {start_year}-{str(start_year + 1)[-2:]}"

def build_yearly_summary_with_proxy(rdf, proxy_years_if_single=2, decline=0.05):
    df = rdf.copy()
    df["FY Start"] = df["Period"].apply(get_fy_start)
    df = df[df["FY Start"].notna()].copy()
    if df.empty:
        return pd.DataFrame()

    grouped = df.groupby("FY Start", as_index=False).agg({
        "Total Emissions (tCO2e)": "sum",
        "Energy Usage (GJ)": "sum"
    })
    s1 = df[df["Scope"] == "Scope 1"].groupby("FY Start")["Total Emissions (tCO2e)"].sum()
    s2 = df[df["Scope"] == "Scope 2"].groupby("FY Start")["Total Emissions (tCO2e)"].sum()
    grouped["Scope 1"] = grouped["FY Start"].map(s1).fillna(0.0)
    grouped["Scope 2"] = grouped["FY Start"].map(s2).fillna(0.0)
    grouped["Data Type"] = "Actual"

    rows = {int(r["FY Start"]): r.to_dict() for _, r in grouped.iterrows()}
    actual_years = sorted(rows.keys())
    min_year, max_year = min(actual_years), max(actual_years)

    for y in range(max_year - 1, min_year - 1, -1):
        if y not in rows and (y + 1) in rows:
            src = rows[y + 1]
            rows[y] = {
                "FY Start": y,
                "Total Emissions (tCO2e)": src["Total Emissions (tCO2e)"] * (1 - decline),
                "Energy Usage (GJ)": src["Energy Usage (GJ)"] * (1 - decline),
                "Scope 1": src["Scope 1"] * (1 - decline),
                "Scope 2": src["Scope 2"] * (1 - decline),
                "Data Type": "Proxy"
            }

    if len(actual_years) == 1:
        latest = actual_years[0]
        for i in range(1, proxy_years_if_single + 1):
            y = latest - i
            src = rows[y + 1]
            rows[y] = {
                "FY Start": y,
                "Total Emissions (tCO2e)": src["Total Emissions (tCO2e)"] * (1 - decline),
                "Energy Usage (GJ)": src["Energy Usage (GJ)"] * (1 - decline),
                "Scope 1": src["Scope 1"] * (1 - decline),
                "Scope 2": src["Scope 2"] * (1 - decline),
                "Data Type": "Proxy"
            }

    out = pd.DataFrame(rows.values()).sort_values("FY Start").reset_index(drop=True)
    out["Period"] = out["FY Start"].apply(format_fy)
    return out

# --- Main Logic ---
st.title("💎 ESG AutoPilot - GHG SCOPE 1 AND 2 EMISSIONS CALCULATOR")
st.markdown("**INSTRUCTIONS:** DOWNLOAD TEMPLATE. ENTER THE YEAR AND DATA (CURRENTLY SUPPORTS ONLY ANNUAL DATA) AND GET YOUR EMISSIONS FILE VERIFIABLE EXCEL IMMEDIATELY WHICH IS AUDIT READY")
key = st.sidebar.text_input("Gemini API Key", type="password")
up_files = st.file_uploader("Upload Data Batch", type=['xlsx', 'xls', 'csv', 'pdf', 'png', 'jpg'], accept_multiple_files=True)

if up_files:
    all_rows = []
    for up in up_files:
        data = up.read()
        ftype = up.name.split('.')[-1].lower()
        f_p = extract_period_metadata(up.name)
        
        if ftype in ['pdf', 'png', 'jpg']:
            st.warning(f"⚠️ PDF/Image processing is not yet implemented. Skipping: {up.name}. Please upload Excel (.xlsx/.xls) or CSV files.")
        else:
            with st.spinner(f"Ingesting: {up.name}"):
                try:
                    xl = pd.ExcelFile(io.BytesIO(data)) if ftype != 'csv' else None
                    sheets = xl.sheet_names if xl else [None]
                    for s in sheets:
                        if should_skip_sheet(s):
                            st.info(f"Skipping summary sheet '{s}' in {up.name}.")
                            continue
                        try:
                            raw_df = pd.read_csv(io.BytesIO(data), header=None) if ftype == 'csv' else pd.read_excel(io.BytesIO(data), sheet_name=s, header=None)
                            s_p = extract_period_metadata(s) if s else ""
                            sheet_context = str(s).lower() if s else ""
                            if "electricity" in sheet_context:
                                all_rows.extend(process_electricity_sheet(raw_df, s_p or f_p))
                                continue
                            sub_tables = split_sheet_into_tables(raw_df)
                            for table in sub_tables:
                                try:
                                    rows = process_table_block(table, s_p or f_p, sheet_context)
                                    all_rows.extend(rows)
                                except Exception as e:
                                    st.warning(f"Could not process a table block in sheet '{s}' of {up.name}: {e}")
                        except Exception as e:
                            st.warning(f"Could not read sheet '{s}' from {up.name}: {e}")
                except Exception as e:
                    st.error(f"Failed to open {up.name}: {e}")

    if all_rows:
        rdf = pd.DataFrame(all_rows)
        rdf, removed_double = remove_double_ingested_rows(rdf)
        if removed_double:
            st.warning(
                f"Removed {removed_double} suspected double-ingested row(s) where pre-calculated tCO2e was treated as raw quantity."
            )
        if "Validation Notes" in rdf.columns:
            flagged = rdf[rdf["Validation Notes"].fillna("OK") != "OK"]
            if not flagged.empty:
                st.warning(f"Validation flags found in {len(flagged)} row(s). Check the 'Validation Notes' column in Audit Trail.")
        yearly = build_yearly_summary_with_proxy(rdf)
        if not yearly.empty:
            latest_row = yearly.iloc[-1]
            prev_row = yearly.iloc[-2] if len(yearly) > 1 else None
            latest_p = latest_row["Period"]
            mdf = rdf[rdf["Period"] == latest_p]
            if mdf.empty:
                mdf = rdf[rdf["Period"].apply(lambda x: get_fy_start(x) == latest_row["FY Start"])]
        else:
            unique_p = sorted(rdf['Period'].unique())
            latest_p = unique_p[-1] if unique_p else "All Time"
            mdf = rdf[rdf['Period'] == latest_p] if latest_p != "All Time" else rdf
            latest_row = None
            prev_row = None

        st.markdown(
            f"""
            <div class="hero">
                <h2>Emissions Dashboard</h2>
                <p>Latest reporting period: <b>{latest_p}</b></p>
            </div>
            """,
            unsafe_allow_html=True
        )

        d_total = None if prev_row is None else latest_row["Total Emissions (tCO2e)"] - prev_row["Total Emissions (tCO2e)"]
        d_energy = None if prev_row is None else latest_row["Energy Usage (GJ)"] - prev_row["Energy Usage (GJ)"]

        renewable_elec_types = {"Renewable Electricity (kWh)", "Solar (kWh)", "Wind (kWh)", "Hydel (kWh)", "Custom Renewable Electricity (kWh)"}
        renewable_fuel_types = {"Biofuel (KL)", "Biodiesel (KL)", "Briquettes (Kg)"}
        grid_types = {"Grid Electricity (kWh)"}

        total_em = mdf['Total Emissions (tCO2e)'].sum()
        scope1_em = mdf[mdf['Scope'] == 'Scope 1']['Total Emissions (tCO2e)'].sum()
        scope2_em = mdf[mdf['Scope'] == 'Scope 2']['Total Emissions (tCO2e)'].sum()
        pct_basis_df = mdf.copy()
        if not pct_basis_df[pct_basis_df['Fuel / Electricity Type'].isin(renewable_elec_types)].shape[0] and rdf[rdf['Fuel / Electricity Type'].isin(renewable_elec_types)].shape[0]:
            pct_basis_df = rdf.copy()
            pct_basis_label = "All years (latest FY has no renewable-electricity rows)"
        else:
            pct_basis_label = f"{latest_p}"

        pct_basis_df["_Energy_GJ_Calc"] = pct_basis_df.apply(energy_gj_from_row, axis=1)
        total_gj = pct_basis_df["_Energy_GJ_Calc"].sum()
        ren_elec_gj = pct_basis_df[pct_basis_df['Fuel / Electricity Type'].isin(renewable_elec_types)]["_Energy_GJ_Calc"].sum()
        ren_fuel_gj = pct_basis_df[pct_basis_df['Fuel / Electricity Type'].isin(renewable_fuel_types)]["_Energy_GJ_Calc"].sum()
        renewable_gj = ren_elec_gj + ren_fuel_gj
        non_renewable_gj = max(total_gj - renewable_gj, 0.0)

        def pct(part, whole):
            return 0.0 if whole == 0 else (part / whole) * 100

        def card_html(label, value, delta=None, sub=False):
            indent_cls = " sub-indent" if sub else ""
            delta_html = f"<div class='delta'>{delta}</div>" if delta else ""
            return f"<div class='highlight-card{indent_cls}'><div class='label'>{label}</div><div class='value'>{value}</div>{delta_html}</div>"

        renewable_energy_pct = pct(renewable_gj, total_gj)
        renewable_fuel_base = pct_basis_df[pct_basis_df['Fuel / Electricity Type'].isin(renewable_fuel_types | {"HSD (KL)", "HSD Mobile (KL)", "Furnace Oil (KL)", "LDO (KL)", "Natural Gas (SCM)", "LPG (Kg)", "LSHS (KL)", "Petrol (KL)"})]["_Energy_GJ_Calc"].sum()
        renewable_fuel_pct = pct(ren_fuel_gj, renewable_fuel_base)
        renewable_electricity_base = pct_basis_df[pct_basis_df['Fuel / Electricity Type'].isin(renewable_elec_types | grid_types)]["_Energy_GJ_Calc"].sum()
        renewable_electricity_pct = pct(ren_elec_gj, renewable_electricity_base)

        tabs = st.tabs(["Highlights", "Year-wise Trends", "Scope Analysis", "Energy Mix", "Renewable KPIs", "Audit Trail"])

        with tabs[0]:
            c1, c2, c3 = st.columns(3)
            with c1:
                st.markdown(card_html("Total Emissions (tCO2e)", f"{total_em:,.2f}", None if d_total is None else f"{d_total:,.2f} vs prev FY"), unsafe_allow_html=True)
                st.markdown(card_html("Scope 1", f"{scope1_em:,.2f} tCO2e", sub=True), unsafe_allow_html=True)
                st.markdown(card_html("Scope 2", f"{scope2_em:,.2f} tCO2e", sub=True), unsafe_allow_html=True)
            with c2:
                st.markdown(card_html("Total Energy (GJ)", f"{total_gj:,.0f}", None if d_energy is None else f"{d_energy:,.0f} vs prev FY"), unsafe_allow_html=True)
                st.markdown(card_html("Renewable GJ", f"{renewable_gj:,.0f}", sub=True), unsafe_allow_html=True)
                st.markdown(card_html("Non-Renewable GJ", f"{non_renewable_gj:,.0f}", sub=True), unsafe_allow_html=True)
            with c3:
                st.markdown(card_html("Renewable Energy %", f"{renewable_energy_pct:,.2f}%"), unsafe_allow_html=True)
                st.markdown(card_html("Renewable Fuel %", f"{renewable_fuel_pct:,.2f}%"), unsafe_allow_html=True)
                st.markdown(card_html("Renewable Electricity %", f"{renewable_electricity_pct:,.2f}%"), unsafe_allow_html=True)

        with tabs[1]:
            if not yearly.empty:
                trend_df = yearly[["Period", "Total Emissions (tCO2e)", "Scope 1", "Scope 2", "Energy Usage (GJ)"]].set_index("Period")
                c1, c2 = st.columns(2)
                with c1:
                    st.markdown("`Emissions Trend (tCO2e)`")
                    st.line_chart(trend_df[["Total Emissions (tCO2e)", "Scope 1", "Scope 2"]], use_container_width=True)
                with c2:
                    st.markdown("`Energy Trend (GJ)`")
                    st.bar_chart(trend_df[["Energy Usage (GJ)"]], use_container_width=True)
                if (yearly["Data Type"] == "Proxy").any():
                    st.info("Proxy years filled with 5% lower values than the next available FY.")
                st.dataframe(
                    yearly[["Period", "Data Type", "Total Emissions (tCO2e)", "Scope 1", "Scope 2", "Energy Usage (GJ)"]],
                    use_container_width=True,
                    column_config={"Total Emissions (tCO2e)": st.column_config.NumberColumn(format="%,.2f")}
                )
            else:
                st.info("No FY data detected for trend charts.")

        with tabs[2]:
            scope_df = mdf.groupby("Scope", as_index=False)["Total Emissions (tCO2e)"].sum().sort_values("Total Emissions (tCO2e)", ascending=False)
            st.bar_chart(scope_df.set_index("Scope"), use_container_width=True)
            st.dataframe(scope_df, use_container_width=True)

        with tabs[3]:
            mix_df = pd.DataFrame({
                "Energy Type": ["Renewable", "Non-Renewable"],
                "Energy Usage (GJ)": [renewable_gj, non_renewable_gj]
            })
            st.bar_chart(mix_df.set_index("Energy Type"), use_container_width=True)
            fuel_mix = mdf.groupby("Fuel / Electricity Type", as_index=False)["Energy Usage (GJ)"].sum().sort_values("Energy Usage (GJ)", ascending=False).head(12)
            st.dataframe(fuel_mix, use_container_width=True)

        with tabs[4]:
            k1, k2, k3 = st.columns(3)
            k1.metric("Renewable Energy %", f"{renewable_energy_pct:,.2f}%")
            k2.metric("Renewable Fuel %", f"{renewable_fuel_pct:,.2f}%")
            k3.metric("Renewable Electricity %", f"{renewable_electricity_pct:,.2f}%")
            st.caption(f"Percentages are calculated from normalized energy units (kWh/MWh/GJ/KL/kg/SCM) on: {pct_basis_label}.")

        with tabs[5]:
            st.dataframe(
                rdf,
                use_container_width=True,
                column_config={"Total Emissions (tCO2e)": st.column_config.NumberColumn(format="%,.2f"), "Quantity": st.column_config.NumberColumn(format="%,.2f")}
            )
            st.caption("Thousands separators in uploaded numeric cells are supported.")
        
        o = io.BytesIO()
        with pd.ExcelWriter(o, engine='xlsxwriter') as w: rdf.to_excel(w, index=False)
        st.download_button("📥 Download Audit Trail", o.getvalue(), "esg_audit.xlsx")
    else:
        st.info("Upload standard or matrix data to begin.")
        # Debug: show what the pipeline saw so user can diagnose
        with st.expander("Debug: Show raw data from uploaded files"):
            for up in up_files:
                up.seek(0)
                data = up.read()
                ftype = up.name.split('.')[-1].lower()
                if ftype in ['pdf', 'png', 'jpg']:
                    continue
                try:
                    if ftype == 'csv':
                        debug_df = pd.read_csv(io.BytesIO(data), header=None, nrows=10)
                    else:
                        debug_df = pd.read_excel(io.BytesIO(data), header=None, nrows=10)
                    st.write(f"**{up.name}** — first 10 rows (raw):")
                    st.dataframe(debug_df)
                    st.write(f"Shape: {debug_df.shape[0]} rows x {debug_df.shape[1]} columns")
                except Exception as e:
                    st.write(f"**{up.name}** — could not read: {e}")
