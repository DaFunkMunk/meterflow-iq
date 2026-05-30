
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd


SEED = 42
N_FACILITIES = 25
N_METERS = 125
N_DAYS = 60
START_DATE = pd.Timestamp("2026-04-01 00:00:00")
OUTPUT_DIR = Path(__file__).resolve().parents[1] / "sample_data"


def _rng() -> np.random.Generator:
    return np.random.default_rng(SEED)


def _as_yn(value: bool) -> str:
    return "Y" if value else "N"


def _iso(ts: pd.Timestamp | datetime | str) -> str:
    return pd.Timestamp(ts).strftime("%Y-%m-%dT%H:%M:%S")


def build_facility_master(rng: np.random.Generator) -> pd.DataFrame:
    basin_profiles = [
        ("Delaware", "Permian", "TX", 31.85, -103.50),
        ("Delaware", "Permian", "NM", 32.05, -103.85),
        ("DJ", "Rockies", "CO", 40.10, -104.75),
        ("Powder River", "Rockies", "WY", 43.30, -105.55),
        ("South Texas", "Gulf Coast", "TX", 28.90, -98.20),
        ("Uinta", "Rockies", "UT", 40.20, -110.20),
        ("Green River", "Rockies", "WY", 41.60, -109.30),
    ]
    asset_types = [
        "Gas Gathering",
        "Gas Processing",
        "Compressor Station",
        "Produced Water",
        "Oil Gathering",
        "SWD / Disposal",
        "Water Recycling",
    ]
    operators = ["WES Demo Ops", "MeterFlow Midstream", "Third-Party JV", "Acquired Asset Team"]
    source_systems = ["SimulatedSCADA", "FlowCalSim", "ProCountSim", "PI_SIM"]

    rows: list[dict] = []
    for i in range(1, N_FACILITIES + 1):
        basin, region, state, base_lat, base_lon = basin_profiles[(i - 1) % len(basin_profiles)]
        asset_type = asset_types[(i - 1) % len(asset_types)]
        active = i not in {23, 25}
        eff_start = START_DATE - pd.Timedelta(days=int(rng.integers(365, 1800)))
        eff_end = "" if active else (START_DATE - pd.Timedelta(days=int(rng.integers(30, 180)))).date().isoformat()
        facility_id = f"FAC-{i:03d}"
        rows.append(
            {
                "facility_id": facility_id,
                "facility_name": f"{basin} {asset_type} {i:02d}",
                "region": region,
                "basin": basin,
                "state": state,
                "asset_type": asset_type,
                "operator": operators[i % len(operators)],
                "active_flag": _as_yn(active),
                "effective_start_date": eff_start.date().isoformat(),
                "effective_end_date": eff_end,
                "latitude": round(base_lat + float(rng.normal(0, 0.25)), 6),
                "longitude": round(base_lon + float(rng.normal(0, 0.35)), 6),
                "source_system": source_systems[i % len(source_systems)],
            }
        )
    return pd.DataFrame(rows)


def build_meter_master(facilities: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    meter_types = ["Ultrasonic", "Orifice", "Coriolis", "Turbine", "Mag Meter"]
    products = ["GAS", "NGL", "CRUDE", "PRODUCED_WATER"]
    measurement_types = ["VOLUME", "ALLOCATION", "CUSTODY_TRANSFER", "OPERATIONAL"]
    source_systems = ["SimulatedPollingConnector", "FlowCalSim", "ProCountSim", "PI_SIM"]
    polling_platforms = ["OASYS_SIM", "CYGNET_SIM", "IGNITION_SIM", "PI_CONNECTOR_SIM", "MANUAL_UPLOAD"]

    rows: list[dict] = []
    meters_per_facility = N_METERS // len(facilities)
    meter_num = 1

    for _, facility in facilities.iterrows():
        for _ in range(meters_per_facility):
            product = str(rng.choice(products, p=[0.56, 0.14, 0.12, 0.18]))
            if product == "GAS":
                expected_min, expected_max = rng.uniform(80, 180), rng.uniform(850, 1850)
            elif product == "PRODUCED_WATER":
                expected_min, expected_max = rng.uniform(40, 100), rng.uniform(500, 1200)
            elif product == "CRUDE":
                expected_min, expected_max = rng.uniform(20, 70), rng.uniform(250, 700)
            else:
                expected_min, expected_max = rng.uniform(25, 75), rng.uniform(350, 900)

            active = meter_num not in {11, 37, 63, 88, 104, 119}
            install_date = START_DATE - pd.Timedelta(days=int(rng.integers(120, 2400)))
            rows.append(
                {
                    "meter_id": f"MTR-{meter_num:03d}",
                    "facility_id": facility["facility_id"],
                    "meter_name": f"{facility['facility_id']}-Meter-{meter_num:03d}",
                    "meter_type": str(rng.choice(meter_types)),
                    "product": product,
                    "measurement_type": str(rng.choice(measurement_types, p=[0.54, 0.18, 0.16, 0.12])),
                    "custody_transfer_flag": _as_yn(bool(rng.random() < 0.28)),
                    "source_system": str(rng.choice(source_systems)),
                    "polling_platform": str(rng.choice(polling_platforms, p=[0.34, 0.28, 0.16, 0.14, 0.08])),
                    "install_date": install_date.date().isoformat(),
                    "active_flag": _as_yn(active),
                    "expected_min_volume": round(float(expected_min), 2),
                    "expected_max_volume": round(float(expected_max), 2),
                    "sample_interval_minutes": 60,
                }
            )
            meter_num += 1

    return pd.DataFrame(rows)


def build_known_issue_scenarios(meters: pd.DataFrame) -> pd.DataFrame:
    def ids_for_fac(facility_id: str, count: int = 4) -> str:
        vals = meters.loc[meters["facility_id"].eq(facility_id), "meter_id"].head(count).tolist()
        return ";".join(vals)

    scenarios = [
        ("SCN-001", "Snowstorm-style polling outage", "Selected active meters report zero/stale readings because simulated field communication failed.", "FAC-007", ids_for_fac("FAC-007"), "2026-04-12T02:00:00", "2026-04-12T10:00:00", "ZERO_VOLUME", "Polling/device communication outage", "Check upstream polling platform, device status, and whether corrected measurement values were posted."),
        ("SCN-002", "Duplicate ingestion retry", "A retry creates duplicate meter/timestamp records with different raw_reading_id values.", "FAC-003", ids_for_fac("FAC-003", 5), "2026-04-18T00:00:00", "2026-04-18T23:00:00", "DUPLICATE_READING", "Retry/load logic duplicated source records", "Group by meter_id and reading_timestamp; inspect batch retry history."),
        ("SCN-003", "Invalid meter source feed", "Source feed contains meter IDs missing from meter_master.", "FAC-005", "MTR-998;MTR-999", "2026-04-20T08:00:00", "2026-04-20T14:00:00", "INVALID_METER", "Source-system/reference-data mismatch", "Validate source meter IDs against meter_master and update mapping or reject feed."),
        ("SCN-004", "Late-arriving data after report refresh", "Valid readings arrive after the normal reporting refresh window.", "FAC-012", ids_for_fac("FAC-012", 4), "2026-04-26T00:00:00", "2026-04-26T23:00:00", "LATE_ARRIVAL", "Timing/schedule issue", "Compare event, poll, load, and report refresh timestamps."),
        ("SCN-005", "Stale readings from polling source", "Meters repeat stale values and quality codes for several hours.", "FAC-004", ids_for_fac("FAC-004", 4), "2026-05-05T04:00:00", "2026-05-05T12:00:00", "STALE_READING", "Polling source returned stale values", "Check tag freshness, polling logs, and communication status."),
        ("SCN-006", "Future production date", "A small source batch is dated in the future.", "FAC-002", ids_for_fac("FAC-002", 3), "2026-06-03T01:00:00", "2026-06-03T04:00:00", "FUTURE_DATE", "Bad date-window/source-date logic", "Reject future-dated records and review source date mapping."),
        ("SCN-007", "Unapproved FlowCal-style records", "Measurement records are extracted before approval/close.", "FAC-009", ids_for_fac("FAC-009", 5), "2026-04-29T00:00:00", "2026-04-30T23:00:00", "UNAPPROVED_FLOWCAL_RECORD", "Measurement close process incomplete", "Filter reporting to approved/closed records and check close workflow."),
        ("SCN-008", "FlowCal correction mismatch", "Corrected measurement volume materially differs from raw aggregate.", "FAC-011", ids_for_fac("FAC-011", 4), "2026-05-10T00:00:00", "2026-05-10T23:00:00", "FLOWCAL_MISMATCH", "Measurement correction/reconciliation issue", "Compare raw aggregate to measured and corrected volume by meter/date."),
        ("SCN-009", "Partial publisher load", "Publisher step has rejected records and partial success status.", "FAC-015", ids_for_fac("FAC-015", 3), "2026-05-12T00:00:00", "2026-05-12T23:00:00", "PARTIAL_LOAD", "Target load rejected a subset of records", "Review publisher error rows, rejected count, and target acceptance rules."),
        ("SCN-010", "Facility mapping mismatch", "Source facility assignment conflicts with meter_master.", "FAC-018", ids_for_fac("FAC-018", 4), "2026-05-15T08:00:00", "2026-05-15T18:00:00", "FACILITY_MISMATCH", "Master/reference mapping conflict", "Validate meter-to-facility mapping and effective dates."),
        ("SCN-011", "Invalid quality code", "Source feed contains quality codes outside accepted values.", "FAC-020", ids_for_fac("FAC-020", 4), "2026-05-18T06:00:00", "2026-05-18T14:00:00", "INVALID_QUALITY_CODE", "Source/status code mapping issue", "Review accepted values and source code translation table."),
        ("SCN-012", "Inactive meter still reporting", "Inactive meters continue to produce readings.", "FAC-008", "MTR-037;MTR-063", "2026-04-22T00:00:00", "2026-04-24T23:00:00", "INACTIVE_METER_REPORTING", "Effective-date or active flag mismatch", "Confirm meter status and effective dates before including in reports."),
        ("SCN-013", "Null volume batch", "A small set of active meters sends null volumes.", "FAC-014", ids_for_fac("FAC-014", 3), "2026-05-21T02:00:00", "2026-05-21T07:00:00", "NULL_VOLUME", "Source feed missing measurement values", "Check source payload and apply null-volume exception handling."),
        ("SCN-014", "Negative volume outliers", "A small number of impossible negative values appear in raw readings.", "FAC-021", ids_for_fac("FAC-021", 3), "2026-05-23T10:00:00", "2026-05-23T16:00:00", "NEGATIVE_VOLUME", "Bad sensor/source calculation", "Reject negative values and review source calculation."),
        ("SCN-015", "High nomination variance", "Actual measured volume materially differs from nominated volume.", "FAC-006", ids_for_fac("FAC-006", 5), "2026-05-26T00:00:00", "2026-05-26T23:00:00", "NOMINATION_VARIANCE", "Business variance or data-quality impact", "Compare actual vs nominated by facility/date/product and review exception impact."),
    ]
    return pd.DataFrame(
        scenarios,
        columns=[
            "scenario_id",
            "scenario_name",
            "description",
            "affected_facility_id",
            "affected_meter_ids",
            "start_datetime",
            "end_datetime",
            "expected_exception_type",
            "expected_root_cause",
            "recommended_next_step",
        ],
    )


def _hourly_batch_id(ts: pd.Timestamp) -> str:
    return f"BATCH-{ts.strftime('%Y%m%d-%H')}"


def build_raw_polling_readings(
    meters: pd.DataFrame,
    scenarios: pd.DataFrame,
    rng: np.random.Generator,
) -> pd.DataFrame:
    timestamps = pd.date_range(START_DATE, periods=N_DAYS * 24, freq="h")
    frames: list[pd.DataFrame] = []

    for _, meter in meters.iterrows():
        n = len(timestamps)
        expected_min = float(meter["expected_min_volume"])
        expected_max = float(meter["expected_max_volume"])
        baseline = (expected_min + expected_max) / 2.0
        hours = np.arange(n)
        daily_cycle = 1.0 + 0.07 * np.sin((hours % 24) / 24 * 2 * np.pi)
        weekly_cycle = 1.0 + 0.03 * np.sin(hours / (24 * 7) * 2 * np.pi)
        noise = rng.normal(1.0, 0.08, n)
        volume = np.maximum(0, baseline * daily_cycle * weekly_cycle * noise)

        pressure = rng.normal(780, 80, n)
        temperature = rng.normal(72, 12, n)
        quality = rng.choice(["GOOD", "GOOD", "GOOD", "ESTIMATED", "QUESTIONABLE"], size=n, p=[0.62, 0.18, 0.14, 0.04, 0.02])
        raw_status = np.where(quality == "GOOD", "VALID", "REVIEW")

        poll_delay = pd.to_timedelta(rng.integers(1, 6, n), unit="m")
        load_delay = pd.to_timedelta(rng.integers(2, 21, n), unit="m")
        poll_ts = timestamps + poll_delay
        load_ts = poll_ts + load_delay

        df = pd.DataFrame(
            {
                "meter_id": meter["meter_id"],
                "facility_id": meter["facility_id"],
                "reading_timestamp": timestamps,
                "production_date": timestamps.date.astype(str),
                "gas_day": timestamps.date.astype(str),
                "poll_timestamp": poll_ts,
                "source_system": meter["source_system"],
                "polling_platform": meter["polling_platform"],
                "volume": volume.round(3),
                "pressure": pressure.round(2),
                "temperature": temperature.round(2),
                "quality_code": quality,
                "raw_status": raw_status,
                "load_timestamp": load_ts,
                "batch_id": [_hourly_batch_id(ts) for ts in timestamps],
            }
        )
        frames.append(df)

    raw = pd.concat(frames, ignore_index=True)
    raw.insert(0, "raw_reading_id", [f"RAW-{i:09d}" for i in range(1, len(raw) + 1)])

    # Scenario helpers
    def scenario(sid: str) -> pd.Series:
        return scenarios.loc[scenarios["scenario_id"].eq(sid)].iloc[0]

    def meters_from_scn(sid: str) -> list[str]:
        value = str(scenario(sid)["affected_meter_ids"])
        return [x for x in value.split(";") if x]

    def mask_for(sid: str) -> pd.Series:
        scn = scenario(sid)
        start = pd.Timestamp(scn["start_datetime"])
        end = pd.Timestamp(scn["end_datetime"])
        return raw["meter_id"].isin(meters_from_scn(sid)) & raw["reading_timestamp"].between(start, end)

    # SCN-001 zero/stale outage
    mask = mask_for("SCN-001")
    raw.loc[mask, ["volume", "pressure"]] = [0.0, np.nan]
    raw.loc[mask, "quality_code"] = "STALE"
    raw.loc[mask, "raw_status"] = "NO_SIGNAL"

    # SCN-004 late arrival
    mask = mask_for("SCN-004")
    raw.loc[mask, "load_timestamp"] = raw.loc[mask, "reading_timestamp"] + pd.Timedelta(hours=36)
    raw.loc[mask, "raw_status"] = "LATE_ARRIVAL"

    # SCN-005 stale
    mask = mask_for("SCN-005")
    raw.loc[mask, "quality_code"] = "STALE"
    raw.loc[mask, "raw_status"] = "STALE"

    # SCN-010 facility mismatch
    mask = mask_for("SCN-010")
    wrong_facility = "FAC-001"
    raw.loc[mask, "facility_id"] = wrong_facility
    raw.loc[mask, "raw_status"] = "FACILITY_MISMATCH"

    # SCN-011 invalid quality
    mask = mask_for("SCN-011")
    raw.loc[mask, "quality_code"] = "BAD_CODE_X"
    raw.loc[mask, "raw_status"] = "INVALID_QUALITY_CODE"

    # SCN-013 null volume
    mask = mask_for("SCN-013")
    raw.loc[mask, "volume"] = np.nan
    raw.loc[mask, "raw_status"] = "NULL_VOLUME"

    # SCN-014 negative volume
    mask = mask_for("SCN-014")
    raw.loc[mask, "volume"] = -1 * np.abs(raw.loc[mask, "volume"])
    raw.loc[mask, "raw_status"] = "NEGATIVE_VOLUME"

    # SCN-002 duplicate records: copy a small subset of one day and give new raw_reading_id/event identity.
    dup_mask = mask_for("SCN-002") & raw["reading_timestamp"].dt.hour.isin([6, 7, 8, 9, 10, 11])
    dup_rows = raw.loc[dup_mask].sample(n=min(120, int(dup_mask.sum())), random_state=SEED).copy()
    dup_rows["raw_status"] = "DUPLICATE_RETRY"
    dup_rows["batch_id"] = "BATCH-DUPLICATE-RETRY-SCN-002"
    dup_rows["load_timestamp"] = dup_rows["load_timestamp"] + pd.Timedelta(minutes=45)

    # SCN-003 invalid meter records
    invalid_records = []
    scn3 = scenario("SCN-003")
    invalid_times = pd.date_range(pd.Timestamp(scn3["start_datetime"]), pd.Timestamp(scn3["end_datetime"]), freq="h")
    for idx, ts in enumerate(invalid_times):
        for invalid_meter in ["MTR-998", "MTR-999"]:
            invalid_records.append(
                {
                    "raw_reading_id": "",
                    "meter_id": invalid_meter,
                    "facility_id": "FAC-005",
                    "reading_timestamp": ts,
                    "production_date": ts.date().isoformat(),
                    "gas_day": ts.date().isoformat(),
                    "poll_timestamp": ts + pd.Timedelta(minutes=3),
                    "source_system": "SimulatedPollingConnector",
                    "polling_platform": "OASYS_SIM",
                    "volume": round(float(rng.uniform(100, 900)), 3),
                    "pressure": round(float(rng.normal(760, 60)), 2),
                    "temperature": round(float(rng.normal(75, 8)), 2),
                    "quality_code": "GOOD",
                    "raw_status": "INVALID_METER",
                    "load_timestamp": ts + pd.Timedelta(minutes=12),
                    "batch_id": "BATCH-INVALID-METER-SCN-003",
                }
            )

    # SCN-006 future date records
    scn6 = scenario("SCN-006")
    future_records = []
    future_times = pd.date_range(pd.Timestamp(scn6["start_datetime"]), pd.Timestamp(scn6["end_datetime"]), freq="h")
    for ts in future_times:
        for meter_id in meters_from_scn("SCN-006"):
            meter = meters.loc[meters["meter_id"].eq(meter_id)].iloc[0]
            future_records.append(
                {
                    "raw_reading_id": "",
                    "meter_id": meter_id,
                    "facility_id": meter["facility_id"],
                    "reading_timestamp": ts,
                    "production_date": ts.date().isoformat(),
                    "gas_day": ts.date().isoformat(),
                    "poll_timestamp": ts + pd.Timedelta(minutes=4),
                    "source_system": meter["source_system"],
                    "polling_platform": meter["polling_platform"],
                    "volume": round(float(rng.uniform(80, 600)), 3),
                    "pressure": round(float(rng.normal(760, 60)), 2),
                    "temperature": round(float(rng.normal(74, 8)), 2),
                    "quality_code": "GOOD",
                    "raw_status": "FUTURE_DATE",
                    "load_timestamp": ts + pd.Timedelta(minutes=15),
                    "batch_id": "BATCH-FUTURE-DATE-SCN-006",
                }
            )

    extra = pd.concat([dup_rows, pd.DataFrame(invalid_records), pd.DataFrame(future_records)], ignore_index=True)
    start_id = len(raw) + 1
    extra["raw_reading_id"] = [f"RAW-{i:09d}" for i in range(start_id, start_id + len(extra))]
    raw = pd.concat([raw, extra], ignore_index=True)

    # Convert timestamps to ISO strings for CSV output.
    for col in ["reading_timestamp", "poll_timestamp", "load_timestamp"]:
        raw[col] = pd.to_datetime(raw[col]).dt.strftime("%Y-%m-%dT%H:%M:%S")

    return raw


def build_flowcal_measurement_extract(
    raw: pd.DataFrame,
    meters: pd.DataFrame,
    scenarios: pd.DataFrame,
    rng: np.random.Generator,
) -> pd.DataFrame:
    known_meters = set(meters["meter_id"])
    raw_known = raw[raw["meter_id"].isin(known_meters)].copy()
    raw_known["production_date_dt"] = pd.to_datetime(raw_known["production_date"], errors="coerce")
    raw_known = raw_known[raw_known["production_date_dt"].dt.date.between(START_DATE.date(), (START_DATE + pd.Timedelta(days=N_DAYS - 1)).date())]
    raw_known["volume_num"] = pd.to_numeric(raw_known["volume"], errors="coerce")
    raw_known["is_null_volume"] = raw_known["volume_num"].isna()
    raw_known["is_zero_volume"] = raw_known["volume_num"].eq(0)
    raw_known["is_negative_volume"] = raw_known["volume_num"].lt(0)
    raw_known["is_stale"] = raw_known["quality_code"].eq("STALE")
    raw_known["is_bad_quality"] = ~raw_known["quality_code"].isin(["GOOD", "ESTIMATED", "QUESTIONABLE", "STALE", "MISSING"])

    grouped = raw_known.groupby(["meter_id", "facility_id", "production_date"], as_index=False).agg(
        measured_volume=("volume_num", "sum"),
        null_count=("is_null_volume", "sum"),
        zero_count=("is_zero_volume", "sum"),
        negative_count=("is_negative_volume", "sum"),
        stale_count=("is_stale", "sum"),
        bad_quality_count=("is_bad_quality", "sum"),
    )

    # Ensure one meter-day record for each meter/day with expected 7,500 rows.
    all_days = pd.date_range(START_DATE, periods=N_DAYS, freq="D").date.astype(str)
    base = meters[["meter_id", "facility_id"]].merge(pd.DataFrame({"production_date": all_days}), how="cross")
    fc = base.merge(grouped, on=["meter_id", "facility_id", "production_date"], how="left")
    for col in ["measured_volume", "null_count", "zero_count", "negative_count", "stale_count", "bad_quality_count"]:
        fc[col] = fc[col].fillna(0)

    fc["corrected_volume"] = (fc["measured_volume"] * rng.normal(1.0, 0.012, len(fc))).round(3)
    fc["measured_volume"] = fc["measured_volume"].round(3)
    fc["estimated_flag"] = np.where((fc["null_count"] > 0) | (fc["zero_count"] > 0) | (fc["stale_count"] > 0), "Y", "N")

    exception_conditions = [
        (fc["null_count"] > 0, "NULL_VOLUME"),
        (fc["zero_count"] > 0, "ZERO_VOLUME"),
        (fc["negative_count"] > 0, "NEGATIVE_VOLUME"),
        (fc["stale_count"] > 0, "STALE_READING"),
        (fc["bad_quality_count"] > 0, "INVALID_QUALITY_CODE"),
    ]
    fc["exception_code"] = ""
    for cond, code in exception_conditions:
        fc.loc[cond & fc["exception_code"].eq(""), "exception_code"] = code

    fc["validation_status"] = np.where(fc["exception_code"].eq(""), "VALIDATED", "EXCEPTION")
    fc["approved_flag"] = "Y"
    fc["close_status"] = "CLOSED"

    # Unapproved FlowCal scenario.
    scn7 = scenarios.loc[scenarios["scenario_id"].eq("SCN-007")].iloc[0]
    scn7_meters = [m for m in str(scn7["affected_meter_ids"]).split(";") if m]
    scn7_dates = pd.date_range(pd.Timestamp(scn7["start_datetime"]).date(), pd.Timestamp(scn7["end_datetime"]).date(), freq="D").date.astype(str)
    mask7 = fc["meter_id"].isin(scn7_meters) & fc["production_date"].isin(scn7_dates)
    fc.loc[mask7, "validation_status"] = "PENDING_APPROVAL"
    fc.loc[mask7, "exception_code"] = "UNAPPROVED_FLOWCAL_RECORD"
    fc.loc[mask7, "approved_flag"] = "N"
    fc.loc[mask7, "close_status"] = "OPEN"

    # FlowCal mismatch scenario.
    scn8 = scenarios.loc[scenarios["scenario_id"].eq("SCN-008")].iloc[0]
    scn8_meters = [m for m in str(scn8["affected_meter_ids"]).split(";") if m]
    scn8_dates = pd.date_range(pd.Timestamp(scn8["start_datetime"]).date(), pd.Timestamp(scn8["end_datetime"]).date(), freq="D").date.astype(str)
    mask8 = fc["meter_id"].isin(scn8_meters) & fc["production_date"].isin(scn8_dates)
    fc.loc[mask8, "corrected_volume"] = (fc.loc[mask8, "measured_volume"] * 1.28).round(3)
    fc.loc[mask8, "validation_status"] = "CORRECTED"
    fc.loc[mask8, "exception_code"] = "FLOWCAL_MISMATCH"

    fc = fc.sort_values(["meter_id", "production_date"]).reset_index(drop=True)
    fc.insert(0, "flowcal_record_id", [f"FCAL-{i:08d}" for i in range(1, len(fc) + 1)])
    fc["gas_day"] = fc["production_date"]
    fc["flowcal_batch_id"] = "FCAL-" + pd.to_datetime(fc["production_date"]).dt.strftime("%Y%m%d")
    fc["extracted_timestamp"] = (pd.to_datetime(fc["production_date"]) + pd.Timedelta(days=1, hours=6)).dt.strftime("%Y-%m-%dT%H:%M:%S")
    fc["last_updated_timestamp"] = (pd.to_datetime(fc["production_date"]) + pd.Timedelta(days=1, hours=7)).dt.strftime("%Y-%m-%dT%H:%M:%S")

    return fc[
        [
            "flowcal_record_id",
            "meter_id",
            "facility_id",
            "production_date",
            "gas_day",
            "measured_volume",
            "corrected_volume",
            "estimated_flag",
            "validation_status",
            "exception_code",
            "approved_flag",
            "close_status",
            "flowcal_batch_id",
            "extracted_timestamp",
            "last_updated_timestamp",
        ]
    ]


def build_nominations_daily(
    facilities: pd.DataFrame,
    flowcal: pd.DataFrame,
    scenarios: pd.DataFrame,
    rng: np.random.Generator,
) -> pd.DataFrame:
    daily_actual = flowcal.groupby(["facility_id", "production_date"], as_index=False)["corrected_volume"].sum()
    all_days = pd.date_range(START_DATE, periods=N_DAYS, freq="D").date.astype(str)
    base = facilities[["facility_id"]].merge(pd.DataFrame({"production_date": all_days}), how="cross")
    nom = base.merge(daily_actual, on=["facility_id", "production_date"], how="left")
    nom["corrected_volume"] = nom["corrected_volume"].fillna(0)
    products = ["GAS", "NGL", "CRUDE", "PRODUCED_WATER"]
    customer_groups = ["Commercial", "Operations", "Affiliate", "Third-Party", "Internal Forecast"]
    contract_types = ["Firm", "Interruptible", "Gathering", "Processing", "Water Services"]
    nom["product"] = rng.choice(products, len(nom), p=[0.55, 0.15, 0.12, 0.18])
    nom["customer_group"] = rng.choice(customer_groups, len(nom))
    nom["contract_type"] = rng.choice(contract_types, len(nom))
    nom["nominated_volume"] = (nom["corrected_volume"] * rng.normal(1.0, 0.08, len(nom))).round(3)
    nom["status"] = rng.choice(["ACTIVE", "ACTIVE", "ACTIVE", "REVISED", "PENDING"], len(nom), p=[0.55, 0.25, 0.13, 0.05, 0.02])
    nom["effective_start_date"] = nom["production_date"]
    nom["effective_end_date"] = nom["production_date"]

    # High nomination variance scenario.
    scn15 = scenarios.loc[scenarios["scenario_id"].eq("SCN-015")].iloc[0]
    scn15_dates = pd.date_range(pd.Timestamp(scn15["start_datetime"]).date(), pd.Timestamp(scn15["end_datetime"]).date(), freq="D").date.astype(str)
    mask15 = nom["facility_id"].eq(scn15["affected_facility_id"]) & nom["production_date"].isin(scn15_dates)
    nom.loc[mask15, "nominated_volume"] = (nom.loc[mask15, "corrected_volume"] * 1.65).round(3)
    nom.loc[mask15, "status"] = "ACTIVE"

    nom = nom.sort_values(["facility_id", "production_date"]).reset_index(drop=True)
    nom.insert(0, "nomination_id", [f"NOM-{i:08d}" for i in range(1, len(nom) + 1)])
    return nom[
        [
            "nomination_id",
            "facility_id",
            "production_date",
            "product",
            "nominated_volume",
            "customer_group",
            "contract_type",
            "status",
            "effective_start_date",
            "effective_end_date",
        ]
    ]


def build_support_tickets(
    facilities: pd.DataFrame,
    meters: pd.DataFrame,
    scenarios: pd.DataFrame,
    rng: np.random.Generator,
) -> pd.DataFrame:
    issue_types = [
        "BI Report Mismatch",
        "Missing Measurement Data",
        "Late Data Arrival",
        "Duplicate Records",
        "FlowCal Exception",
        "Polling Outage",
        "Failed Pipeline Job",
        "Incorrect Facility Mapping",
        "Invalid Quality Code",
        "User Access Issue",
    ]
    teams = ["Measurement", "Operations", "BI Reporting", "Application Support", "Commercial", "Field Ops"]
    statuses = ["New", "Investigating", "Resolved", "Closed", "Waiting on Source Team"]
    root_causes = [
        "Source Data",
        "Polling Platform",
        "Reference Data",
        "ETL / Pipeline",
        "Measurement Approval",
        "Report Logic",
        "User Workflow",
        "Access / Security",
    ]
    system_names = ["MeterFlow IQ", "FlowCalSim", "BigQuery", "Snowflake", "Azure SQL", "Streamlit", "SimulatedSCADA"]

    # Start with scenario-linked tickets.
    rows: list[dict] = []
    for _, scn in scenarios.iterrows():
        meters_list = [m for m in str(scn["affected_meter_ids"]).split(";") if m]
        open_dt = pd.Timestamp(scn["start_datetime"]) + pd.Timedelta(hours=int(rng.integers(1, 8)))
        status = str(rng.choice(["Investigating", "Resolved", "Closed"], p=[0.25, 0.45, 0.30]))
        close_dt = "" if status == "Investigating" else _iso(open_dt + pd.Timedelta(hours=int(rng.integers(4, 72))))
        severity = "High" if scn["expected_exception_type"] in {"ZERO_VOLUME", "DUPLICATE_READING", "INVALID_METER", "UNAPPROVED_FLOWCAL_RECORD", "FLOWCAL_MISMATCH", "PARTIAL_LOAD"} else str(rng.choice(["Medium", "High"]))
        rows.append(
            {
                "opened_datetime": _iso(open_dt),
                "closed_datetime": close_dt,
                "system_name": str(rng.choice(system_names)),
                "issue_type": str(scn["scenario_name"]),
                "severity": severity,
                "facility_id": scn["affected_facility_id"],
                "meter_id": str(rng.choice(meters_list)) if meters_list else "",
                "reported_by_team": str(rng.choice(teams)),
                "ticket_status": status,
                "business_impact": f"Known scenario: {scn['description']}",
                "root_cause_category": scn["expected_root_cause"],
                "resolution_summary": scn["recommended_next_step"],
                "related_batch_id": f"BATCH-{pd.Timestamp(scn['start_datetime']).strftime('%Y%m%d-%H')}",
                "related_exception_code": scn["expected_exception_type"],
            }
        )

    # Fill out additional random tickets.
    while len(rows) < 350:
        facility = facilities.sample(1, random_state=int(rng.integers(0, 1_000_000))).iloc[0]
        meter_candidates = meters.loc[meters["facility_id"].eq(facility["facility_id"]), "meter_id"].tolist()
        meter_id = str(rng.choice(meter_candidates)) if meter_candidates else ""
        open_dt = START_DATE + pd.Timedelta(days=int(rng.integers(0, N_DAYS)), hours=int(rng.integers(0, 24)), minutes=int(rng.integers(0, 60)))
        status = str(rng.choice(statuses, p=[0.08, 0.15, 0.34, 0.34, 0.09]))
        close_dt = "" if status in {"New", "Investigating", "Waiting on Source Team"} else _iso(open_dt + pd.Timedelta(hours=int(rng.integers(2, 96))))
        issue_type = str(rng.choice(issue_types))
        root = str(rng.choice(root_causes))
        exception_code = str(rng.choice(["NULL_VOLUME", "ZERO_VOLUME", "DUPLICATE_READING", "LATE_ARRIVAL", "STALE_READING", "MISSING_ASSET", "PIPELINE_FAILED", ""]))
        rows.append(
            {
                "opened_datetime": _iso(open_dt),
                "closed_datetime": close_dt,
                "system_name": str(rng.choice(system_names)),
                "issue_type": issue_type,
                "severity": str(rng.choice(["Low", "Medium", "High", "Critical"], p=[0.18, 0.45, 0.30, 0.07])),
                "facility_id": facility["facility_id"],
                "meter_id": meter_id,
                "reported_by_team": str(rng.choice(teams)),
                "ticket_status": status,
                "business_impact": str(rng.choice(["Report output delayed", "Dashboard value mismatch", "Potential measurement close impact", "User unable to validate volume", "Pipeline health alert"])),
                "root_cause_category": root,
                "resolution_summary": str(rng.choice(["Validated source-to-target counts", "Reviewed mapping and confirmed correction", "Waiting on source system refresh", "Documented workaround and escalation path", "Closed after successful rerun"])),
                "related_batch_id": f"BATCH-{open_dt.strftime('%Y%m%d-%H')}",
                "related_exception_code": exception_code,
            }
        )

    tickets = pd.DataFrame(rows).reset_index(drop=True)
    tickets.insert(0, "ticket_id", [f"TCK-{i:06d}" for i in range(1, len(tickets) + 1)])
    return tickets


def build_pipeline_run_log(rng: np.random.Generator) -> pd.DataFrame:
    pipelines = [
        ("bronze_raw_polling_ingest", "raw_polling_readings.csv"),
        ("bronze_mongodb_event_ingest", "meter_polling_events"),
        ("silver_meter_validation", "bronze_meter_readings"),
        ("gold_facility_daily_kpi", "silver_validated_readings"),
        ("publish_bigquery_streamlit", "gold_investigation_tables"),
    ]
    rows: list[dict] = []
    run_dates = pd.date_range(START_DATE + pd.Timedelta(days=30), periods=30, freq="D")
    for day in run_dates:
        for pipeline_name, source_file in pipelines:
            start = day + pd.Timedelta(hours=int(rng.integers(1, 6)), minutes=int(rng.integers(0, 45)))
            duration = int(rng.integers(3, 38))
            completed = start + pd.Timedelta(minutes=duration)
            status = str(rng.choice(["SUCCESS", "SUCCESS", "SUCCESS", "PARTIAL_SUCCESS", "FAILED", "RETRY_SUCCESS"], p=[0.55, 0.20, 0.12, 0.06, 0.04, 0.03]))
            rows_read = int(rng.integers(1_000, 200_000)) if "bronze" in pipeline_name else int(rng.integers(500, 25_000))
            if status == "SUCCESS":
                rows_rejected = int(rng.integers(0, 25))
                error_count = 0
                warning_count = int(rng.integers(0, 12))
                error_msg = ""
            elif status == "RETRY_SUCCESS":
                rows_rejected = int(rng.integers(0, 75))
                error_count = int(rng.integers(1, 4))
                warning_count = int(rng.integers(2, 15))
                error_msg = "Initial attempt failed; retry completed successfully."
            elif status == "PARTIAL_SUCCESS":
                rows_rejected = int(rng.integers(100, 1_200))
                error_count = int(rng.integers(1, 8))
                warning_count = int(rng.integers(8, 30))
                error_msg = "Partial load: rejected rows exceeded warning threshold."
            else:
                rows_rejected = int(rng.integers(500, 2_500))
                error_count = int(rng.integers(2, 12))
                warning_count = int(rng.integers(5, 40))
                error_msg = str(rng.choice(["Source unavailable", "Credential/token expired", "Schema mismatch", "Target write failure", "Rejected rows above threshold"]))
            rows_accepted = max(0, rows_read - rows_rejected)
            rows.append(
                {
                    "pipeline_name": pipeline_name,
                    "source_file": source_file,
                    "started_at": _iso(start),
                    "completed_at": _iso(completed),
                    "status": status,
                    "rows_read": rows_read,
                    "rows_accepted": rows_accepted,
                    "rows_rejected": rows_rejected,
                    "error_count": error_count,
                    "warning_count": warning_count,
                    "error_message": error_msg,
                    "trigger_type": str(rng.choice(["SCHEDULED", "MANUAL", "RETRY"], p=[0.82, 0.10, 0.08])),
                    "environment": "DEV",
                }
            )
    df = pd.DataFrame(rows)
    df.insert(0, "run_id", [f"RUN-{i:06d}" for i in range(1, len(df) + 1)])

    # Force a few named scenarios.
    idx = df.sample(5, random_state=SEED).index
    df.loc[idx[:2], "status"] = "FAILED"
    df.loc[idx[:2], "error_message"] = "Scenario-seeded failure for demo."
    df.loc[idx[2:], "status"] = "PARTIAL_SUCCESS"
    df.loc[idx[2:], "error_message"] = "Scenario-seeded partial load for demo."
    return df


def build_dq_rules_reference() -> pd.DataFrame:
    rules = [
        ("DQ_NULL_VOLUME", "Volume must not be null", "Silver", "volume", "not_null", "volume IS NOT NULL", "High", "Missing measurements can undercount or break downstream reporting.", "Data Engineering", "Y"),
        ("DQ_ZERO_VOLUME", "Active meter zero-volume review", "Silver", "volume", "conditional", "volume = 0 for active meter requires review", "Medium/High", "Zero can be real or a source/polling issue.", "Measurement", "Y"),
        ("DQ_VOLUME_NON_NEGATIVE", "Volume must not be negative", "Silver", "volume", "range", "volume >= 0", "High", "Negative volume is not valid for this simulated measurement.", "Data Engineering", "Y"),
        ("DQ_DUPLICATE_METER_TIMESTAMP", "No duplicate meter timestamp", "Silver", "meter_id,reading_timestamp", "unique_grain", "COUNT(*) by meter_id + reading_timestamp <= 1", "High", "Duplicates can inflate totals.", "Data Engineering", "Y"),
        ("DQ_METER_MUST_EXIST_IN_MASTER", "Meter must exist in master", "Silver", "meter_id", "relationship", "meter_id exists in meter_master", "High", "Unmapped meters can disappear in reports.", "Data Governance", "Y"),
        ("DQ_FACILITY_MUST_MATCH_MASTER", "Facility must match meter master", "Silver", "facility_id", "relationship", "raw facility_id matches meter_master facility_id", "High", "Bad mapping can move volumes to the wrong asset.", "Data Governance", "Y"),
        ("DQ_ACTIVE_METER_ONLY", "Inactive meter reporting review", "Silver", "active_flag", "conditional", "active_flag = Y for reporting records", "Medium/High", "Inactive meters require effective-date review.", "Measurement", "Y"),
        ("DQ_QUALITY_CODE_VALID", "Quality code accepted values", "Silver", "quality_code", "accepted_values", "quality_code in GOOD, ESTIMATED, QUESTIONABLE, STALE, MISSING", "Medium", "Bad status mapping breaks interpretation.", "Data Engineering", "Y"),
        ("DQ_PRODUCTION_DATE_NOT_FUTURE", "Production date cannot be future", "Silver", "production_date", "date_window", "production_date <= current_date", "Medium", "Future dates indicate source/date mapping issue.", "Data Engineering", "Y"),
        ("DQ_LATE_ARRIVAL", "Load must meet reporting window", "Silver", "load_timestamp", "timeliness", "load_timestamp <= expected_report_refresh_time", "Medium", "Late data can cause reports to be wrong at refresh.", "Application Support", "Y"),
        ("DQ_STALE_READING", "Reading freshness check", "Silver", "reading_timestamp,quality_code", "freshness", "quality_code != STALE and reading is within expected interval", "Medium/High", "Stale values can look valid without context.", "Operations", "Y"),
        ("DQ_FLOWCAL_APPROVED_FOR_REPORTING", "FlowCal record approved", "Gold", "approved_flag,close_status", "business_rule", "approved_flag = Y and close_status = CLOSED", "High", "Unapproved measurements should not feed final reports.", "Measurement", "Y"),
        ("DQ_FLOWCAL_CORRECTION_VARIANCE", "FlowCal correction variance", "Gold", "measured_volume,corrected_volume", "threshold", "ABS(corrected - measured) / measured <= threshold", "Medium/High", "Large corrections require measurement review.", "Measurement", "Y"),
        ("DQ_NOMINATION_VARIANCE", "Actual vs nomination variance threshold", "Gold", "actual_volume,nominated_volume", "threshold", "variance within configured threshold", "Medium", "Business users need to understand plan-vs-actual differences.", "Commercial", "Y"),
        ("DQ_PIPELINE_RUN_SUCCESS", "Pipeline run success check", "Gold", "status", "status", "status in SUCCESS, RETRY_SUCCESS", "High", "Failed/partial loads affect downstream trust.", "Application Support", "Y"),
        ("DQ_ROWS_REJECTED_THRESHOLD", "Rejected rows threshold", "Gold", "rows_rejected", "threshold", "rows_rejected <= configured threshold", "Medium/High", "Rejected rows need triage.", "Application Support", "Y"),
        ("DQ_SOURCE_SYSTEM_PRESENT", "Source system must be populated", "Bronze", "source_system", "not_null", "source_system IS NOT NULL", "Medium", "Lineage depends on source metadata.", "Data Governance", "Y"),
        ("DQ_BATCH_ID_PRESENT", "Batch ID must be populated", "Bronze", "batch_id", "not_null", "batch_id IS NOT NULL", "Medium", "Batch metadata supports reconciliation and reruns.", "Data Engineering", "Y"),
        ("DQ_POLL_TIMESTAMP_PRESENT", "Poll timestamp must be populated", "Bronze", "poll_timestamp", "not_null", "poll_timestamp IS NOT NULL", "Medium", "Polling timestamp supports freshness checks.", "Operations", "Y"),
        ("DQ_RAW_PAYLOAD_PRESERVED", "Raw event payload preservation", "Bronze", "raw_payload", "auditability", "raw payload preserved for MongoDB events", "Medium", "Raw context helps explain exceptions.", "Data Governance", "Y"),
    ]
    return pd.DataFrame(
        rules,
        columns=[
            "rule_id",
            "rule_name",
            "layer",
            "field_name",
            "rule_type",
            "condition_description",
            "severity",
            "business_reason",
            "owner_team",
            "active_flag",
        ],
    )


def write_csv(df: pd.DataFrame, filename: str) -> None:
    path = OUTPUT_DIR / filename
    df.to_csv(path, index=False)


def main() -> None:
    rng = _rng()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("Generating facility_master.csv...")
    facilities = build_facility_master(rng)
    write_csv(facilities, "facility_master.csv")

    print("Generating meter_master.csv...")
    meters = build_meter_master(facilities, rng)
    write_csv(meters, "meter_master.csv")

    print("Generating known_issue_scenarios.csv...")
    scenarios = build_known_issue_scenarios(meters)
    write_csv(scenarios, "known_issue_scenarios.csv")

    print("Generating raw_polling_readings.csv...")
    raw = build_raw_polling_readings(meters, scenarios, rng)
    write_csv(raw, "raw_polling_readings.csv")

    print("Generating flowcal_measurement_extract.csv...")
    flowcal = build_flowcal_measurement_extract(raw, meters, scenarios, rng)
    write_csv(flowcal, "flowcal_measurement_extract.csv")

    print("Generating nominations_daily.csv...")
    nominations = build_nominations_daily(facilities, flowcal, scenarios, rng)
    write_csv(nominations, "nominations_daily.csv")

    print("Generating support_tickets.csv...")
    tickets = build_support_tickets(facilities, meters, scenarios, rng)
    write_csv(tickets, "support_tickets.csv")

    print("Generating pipeline_run_log.csv...")
    pipeline_runs = build_pipeline_run_log(rng)
    write_csv(pipeline_runs, "pipeline_run_log.csv")

    print("Generating dq_rules_reference.csv...")
    dq_rules = build_dq_rules_reference()
    write_csv(dq_rules, "dq_rules_reference.csv")

    summary = pd.DataFrame(
        [
            ("facility_master.csv", len(facilities)),
            ("meter_master.csv", len(meters)),
            ("raw_polling_readings.csv", len(raw)),
            ("flowcal_measurement_extract.csv", len(flowcal)),
            ("nominations_daily.csv", len(nominations)),
            ("support_tickets.csv", len(tickets)),
            ("pipeline_run_log.csv", len(pipeline_runs)),
            ("dq_rules_reference.csv", len(dq_rules)),
            ("known_issue_scenarios.csv", len(scenarios)),
        ],
        columns=["file", "rows"],
    )
    print("\nGenerated MeterFlow IQ source CSV package:")
    print(summary.to_string(index=False))
    print(f"\nOutput folder: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
