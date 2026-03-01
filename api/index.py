import sys
import os

from flask import Flask, request, jsonify, Response

app = Flask(__name__)

# Import all processing logic from logic.py (same directory)
_startup_error = None
try:
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from logic import *
except Exception as _e:
    import traceback as _tb
    _startup_error = _tb.format_exc()


@app.route("/api/template", methods=["GET"])
def download_template():
    try:
        data = generate_template()
        return Response(
            data,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=ESG_Data_Template.xlsx"}
        )
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


@app.route("/api/health", methods=["GET"])
def health():
    if _startup_error:
        return jsonify({"error": _startup_error}), 500
    return jsonify({"status": "ok"})


@app.route("/api/process", methods=["POST"])
def process_files():
    if _startup_error:
        return jsonify({"error": "Startup failed", "details": _startup_error}), 500
    try:
        return _do_process()
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


def _do_process():
    files = request.files.getlist("files")
    if not files:
        return jsonify({"error": "No files uploaded"}), 400

    all_rows = []
    warnings = []

    for f in files:
        data = f.read()
        fname = f.filename
        ftype = fname.rsplit(".", 1)[-1].lower()
        f_p = extract_period_metadata(fname)

        if ftype in ["pdf", "png", "jpg", "jpeg"]:
            warnings.append(f"Skipping {fname}: PDF/image processing not yet supported.")
            continue

        try:
            xl = pd.ExcelFile(io.BytesIO(data)) if ftype != "csv" else None
            sheets = xl.sheet_names if xl else [None]
            for s in sheets:
                try:
                    if ftype == "csv":
                        raw_df = pd.read_csv(io.BytesIO(data), header=None)
                    else:
                        raw_df = pd.read_excel(io.BytesIO(data), sheet_name=s, header=None)
                    s_p = extract_period_metadata(s) if s else ""
                    sheet_context = str(s).lower() if s else ""
                    if "electricity" in sheet_context:
                        all_rows.extend(process_electricity_sheet(raw_df, s_p or f_p))
                        continue
                    for table in split_sheet_into_tables(raw_df):
                        try:
                            all_rows.extend(process_table_block(table, s_p or f_p, sheet_context))
                        except Exception as e:
                            warnings.append(f"Table error in sheet '{s}' of {fname}: {e}")
                except Exception as e:
                    warnings.append(f"Cannot read sheet '{s}' from {fname}: {e}")
        except Exception as e:
            warnings.append(f"Failed to open {fname}: {e}")

    if not all_rows:
        return jsonify({"error": "No processable data found.", "warnings": warnings}), 422

    rdf = pd.DataFrame(all_rows)
    yearly = build_yearly_summary_with_proxy(rdf)

    if not yearly.empty:
        latest_row = yearly.iloc[-1]
        prev_row = yearly.iloc[-2] if len(yearly) > 1 else None
        latest_p = latest_row["Period"]
        mdf = rdf[rdf["Period"] == latest_p]
        if mdf.empty:
            mdf = rdf[rdf["Period"].apply(lambda x: get_fy_start(x) == latest_row["FY Start"])]
    else:
        unique_p = sorted(rdf["Period"].unique())
        latest_p = unique_p[-1] if unique_p else "All Time"
        mdf = rdf[rdf["Period"] == latest_p] if latest_p != "All Time" else rdf
        latest_row = None
        prev_row = None

    pct_basis_df = mdf.copy()
    if (not pct_basis_df[pct_basis_df["Fuel / Electricity Type"].isin(RENEWABLE_ELEC_TYPES)].shape[0]
            and rdf[rdf["Fuel / Electricity Type"].isin(RENEWABLE_ELEC_TYPES)].shape[0]):
        pct_basis_df = rdf.copy()

    pct_basis_df["_Energy_GJ_Calc"] = pct_basis_df.apply(energy_gj_from_row, axis=1)
    total_gj = pct_basis_df["_Energy_GJ_Calc"].sum()
    ren_elec_gj = pct_basis_df[pct_basis_df["Fuel / Electricity Type"].isin(RENEWABLE_ELEC_TYPES)]["_Energy_GJ_Calc"].sum()
    ren_fuel_gj = pct_basis_df[pct_basis_df["Fuel / Electricity Type"].isin(RENEWABLE_FUEL_TYPES)]["_Energy_GJ_Calc"].sum()
    renewable_gj = ren_elec_gj + ren_fuel_gj
    non_renewable_gj = max(total_gj - renewable_gj, 0.0)

    total_em = mdf["Total Emissions (tCO2e)"].sum()
    scope1_em = mdf[mdf["Scope"] == "Scope 1"]["Total Emissions (tCO2e)"].sum()
    scope2_em = mdf[mdf["Scope"] == "Scope 2"]["Total Emissions (tCO2e)"].sum()

    ren_fuel_base = pct_basis_df[pct_basis_df["Fuel / Electricity Type"].isin(
        RENEWABLE_FUEL_TYPES | FOSSIL_FUEL_TYPES)]["_Energy_GJ_Calc"].sum()
    ren_elec_base = pct_basis_df[pct_basis_df["Fuel / Electricity Type"].isin(
        RENEWABLE_ELEC_TYPES | GRID_TYPES)]["_Energy_GJ_Calc"].sum()

    yearly_data = []
    if not yearly.empty:
        yearly_data = yearly[["Period", "Data Type", "Total Emissions (tCO2e)",
                               "Scope 1", "Scope 2", "Energy Usage (GJ)"]].to_dict(orient="records")

    scope_data = (
        mdf.groupby("Scope", as_index=False)["Total Emissions (tCO2e)"].sum()
        .sort_values("Total Emissions (tCO2e)", ascending=False)
        .to_dict(orient="records")
    )

    fuel_mix = (
        mdf.groupby("Fuel / Electricity Type", as_index=False)["Energy Usage (GJ)"].sum()
        .sort_values("Energy Usage (GJ)", ascending=False)
        .head(12)
        .to_dict(orient="records")
    )

    audit_records = rdf.to_dict(orient="records")

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        rdf.to_excel(writer, index=False, sheet_name="ESG Audit Trail")
    excel_b64 = base64.b64encode(buf.getvalue()).decode("utf-8")

    return jsonify({
        "latest_period": latest_p,
        "warnings": warnings,
        "kpis": {
            "total_emissions": round(float(total_em), 2),
            "scope1_emissions": round(float(scope1_em), 2),
            "scope2_emissions": round(float(scope2_em), 2),
            "total_energy_gj": round(float(total_gj), 0),
            "renewable_gj": round(float(renewable_gj), 0),
            "non_renewable_gj": round(float(non_renewable_gj), 0),
            "renewable_energy_pct": pct(renewable_gj, total_gj),
            "renewable_fuel_pct": pct(ren_fuel_gj, ren_fuel_base),
            "renewable_electricity_pct": pct(ren_elec_gj, ren_elec_base),
            "delta_total": round(float(latest_row["Total Emissions (tCO2e)"] - prev_row["Total Emissions (tCO2e)"]), 2)
                           if (latest_row is not None and prev_row is not None) else None,
            "delta_energy": round(float(latest_row["Energy Usage (GJ)"] - prev_row["Energy Usage (GJ)"]), 0)
                            if (latest_row is not None and prev_row is not None) else None,
        },
        "yearly_trend": yearly_data,
        "scope_analysis": scope_data,
        "fuel_mix": fuel_mix,
        "audit_trail": audit_records,
        "excel_b64": excel_b64,
    })


