#!/usr/bin/env python3
"""AQI 数据清洗：把流水线各阶段的 CSV 统一成「可训练 / 可复核」的干净表。

对应 README 里的四个数据形态，按表头自动识别（也可用 --kind 指定）：

    openaq      day,average,coverage,unit,parameter      ← retrival_openaq_data.py / integrate_pollutants.py 之前的原始数据
    integrated  datetime,pm25,pm10,o3,co,so2,no2         ← integrate_pollutants.py
    aqi         datetime,pm25,pm25_aqi,...,overall_aqi   ← calculate_aqi.py
    merged      SITE,STATION,DATE,...,AQI                ← merge_all_gsod_aqi_data.py

只依赖标准库：清洗是流水线的最前置一步，不该因为没装 pandas/autogluon 就跑不起来。

用法：
    python clean_aqi_data.py --input aqi_data/bakersfield_aqi_2024_2024.csv --output out.csv
    python clean_aqi_data.py --input openaq_data/x.csv --output y.csv --report y.report.json --min-coverage 75
    python clean_aqi_data.py --input merged.csv --output merged_clean.csv --kind merged

退出码：0 成功；2 输入不可读或表头无法识别。
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from pathlib import Path

KINDS = ("openaq", "integrated", "aqi", "merged")
POLLUTANTS = ("pm25", "pm10", "o3", "co", "so2", "no2")
AQI_SUFFIX = "_aqi"

# 物理上限：超过这个值的浓度只可能是单位错或传感器故障，按缺失处理而不是当成极端污染。
CONCENTRATION_CAP = {"pm25": 1500.0, "pm10": 2000.0, "o3": 2.0, "co": 120.0, "so2": 5.0, "no2": 5.0}
AQI_RANGE = (0.0, 500.0)
# merged 表里的气象列：合法区间，越界按缺失处理（不删行，训练时可用其余特征）。
WEATHER_RANGE = {
    "TEMP": (-90.0, 60.0),
    "DEWP": (-90.0, 40.0),
    "STP": (800.0, 1100.0),
    "VISIB": (0.0, 50.0),
    "WDSP": (0.0, 80.0),
    "MXSPD": (0.0, 120.0),
    "MAX": (-90.0, 60.0),
    "MIN": (-90.0, 60.0),
    "PRCP": (0.0, 500.0),
}

DATE_PATTERNS = (
    re.compile(r"^(\d{4})-(\d{1,2})-(\d{1,2})"),
    re.compile(r"^(\d{4})/(\d{1,2})/(\d{1,2})"),
    re.compile(r"^(\d{4})(\d{2})(\d{2})$"),
)


class CleanError(RuntimeError):
    """输入无法处理时抛出，由 main() 转成退出码 2。"""


def parse_date(raw: str | None) -> str | None:
    """把 day / datetime / DATE 统一成 YYYY-MM-DD；认不出就返回 None。"""
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    for pattern in DATE_PATTERNS:
        match = pattern.match(text)
        if match:
            year, month, day = (int(group) for group in match.groups())
            if 1 <= month <= 12 and 1 <= day <= 31:
                return f"{year:04d}-{month:02d}-{day:02d}"
            return None
    return None


def parse_number(raw: str | None) -> float | None:
    """空串、NA、N/A、非数字一律 None；保留负号，好让调用方按规则判断。"""
    if raw is None:
        return None
    text = str(raw).strip()
    if not text or text.lower() in {"na", "n/a", "nan", "null", "none", "-"}:
        return None
    try:
        value = float(text)
    except ValueError:
        return None
    if value != value or value in (float("inf"), float("-inf")):  # NaN / inf
        return None
    return value


def detect_kind(fieldnames: list[str]) -> str:
    """按表头判断数据形态；判断不出来就报错，绝不猜。"""
    names = {name.strip().lower() for name in fieldnames}
    if {"day", "average"} <= names:
        return "openaq"
    if "site" in names and "date" in names and "aqi" in names:
        return "merged"
    if "datetime" in names:
        if any(f"{p}{AQI_SUFFIX}" in names for p in POLLUTANTS) or "overall_aqi" in names:
            return "aqi"
        if any(p in names for p in POLLUTANTS):
            return "integrated"
    raise CleanError(f"无法识别的表头：{fieldnames}")


def clean_openaq(rows, min_coverage):
    """原始 pollutants：一天一行，average 是日均浓度。"""
    out, dropped = [], {}
    seen: dict[str, int] = {}
    for row in rows:
        date = parse_date(row.get("day"))
        if date is None:
            dropped["bad_date"] = dropped.get("bad_date", 0) + 1
            continue
        value = parse_number(row.get("average"))
        parameter = (row.get("parameter") or "").strip().lower()
        if value is None:
            dropped["missing_value"] = dropped.get("missing_value", 0) + 1
            continue
        if value < 0:
            dropped["negative_value"] = dropped.get("negative_value", 0) + 1
            continue
        cap = CONCENTRATION_CAP.get(parameter)
        if cap is not None and value > cap:
            dropped["above_physical_cap"] = dropped.get("above_physical_cap", 0) + 1
            continue
        coverage = parse_number(row.get("coverage"))
        if min_coverage > 0 and (coverage is None or coverage < min_coverage):
            dropped["low_coverage"] = dropped.get("low_coverage", 0) + 1
            continue
        record = {
            "day": date,
            "average": value,
            "coverage": coverage,
            "unit": (row.get("unit") or "").strip(),
            "parameter": parameter,
        }
        # 同一天重复：留覆盖率更高的那条，覆盖率相同留后出现的（数据文件按时间追加）。
        previous = seen.get(date)
        if previous is None:
            seen[date] = len(out)
            out.append(record)
        else:
            old = out[previous]
            dropped["duplicate_day"] = dropped.get("duplicate_day", 0) + 1
            if (record["coverage"] or -1) >= (old["coverage"] or -1):
                out[previous] = record
    return out, dropped


def clean_integrated(rows, _min_coverage):  # noqa: ARG001 - 统一签名
    """按站整合的六污染物宽表。"""
    out, dropped = [], {}
    index: dict[str, int] = {}
    for row in rows:
        date = parse_date(row.get("datetime"))
        if date is None:
            dropped["bad_date"] = dropped.get("bad_date", 0) + 1
            continue
        record = {"datetime": date}
        has_value = False
        for pollutant in POLLUTANTS:
            value = parse_number(row.get(pollutant))
            if value is not None:
                if value < 0 or value > CONCENTRATION_CAP.get(pollutant, float("inf")):
                    dropped["invalid_concentration"] = dropped.get("invalid_concentration", 0) + 1
                    value = None
                else:
                    has_value = True
            record[pollutant] = value
        if not has_value:
            dropped["empty_row"] = dropped.get("empty_row", 0) + 1
            continue
        previous = index.get(date)
        if previous is None:
            index[date] = len(out)
            out.append(record)
        else:
            dropped["duplicate_day"] = dropped.get("duplicate_day", 0) + 1
            out[previous] = record
    return out, dropped


def clean_aqi(rows, _min_coverage):  # noqa: ARG001 - 统一签名
    """calculate_aqi.py 的产出：浓度 + 分项 AQI + 综合 AQI。"""
    out, dropped = [], {}
    index: dict[str, int] = {}
    columns = ["datetime"]
    for pollutant in POLLUTANTS:
        columns += [pollutant, f"{pollutant}{AQI_SUFFIX}"]
    columns.append("overall_aqi")
    for row in rows:
        date = parse_date(row.get("datetime"))
        if date is None:
            dropped["bad_date"] = dropped.get("bad_date", 0) + 1
            continue
        record = {"datetime": date}
        meaningful = False
        for pollutant in POLLUTANTS:
            concentration = parse_number(row.get(pollutant))
            if concentration is not None and (concentration < 0 or concentration > CONCENTRATION_CAP.get(pollutant, float("inf"))):
                dropped["invalid_concentration"] = dropped.get("invalid_concentration", 0) + 1
                concentration = None
            aqi = parse_number(row.get(f"{pollutant}{AQI_SUFFIX}"))
            if aqi is not None and not (AQI_RANGE[0] <= aqi <= AQI_RANGE[1]):
                dropped["aqi_out_of_range"] = dropped.get("aqi_out_of_range", 0) + 1
                aqi = None
            record[pollutant] = concentration
            record[f"{pollutant}{AQI_SUFFIX}"] = aqi
            meaningful = meaningful or concentration is not None or aqi is not None
        overall = parse_number(row.get("overall_aqi"))
        if overall is not None and not (AQI_RANGE[0] <= overall <= AQI_RANGE[1]):
            dropped["aqi_out_of_range"] = dropped.get("aqi_out_of_range", 0) + 1
            overall = None
        record["overall_aqi"] = overall
        meaningful = meaningful or overall is not None
        if not meaningful:
            dropped["empty_row"] = dropped.get("empty_row", 0) + 1
            continue
        previous = index.get(date)
        if previous is None:
            index[date] = len(out)
            out.append(record)
        else:
            dropped["duplicate_day"] = dropped.get("duplicate_day", 0) + 1
            out[previous] = record
    return out, dropped, columns


def clean_merged(rows, _min_coverage):  # noqa: ARG001 - 统一签名
    """训练表：GSOD 气象列 + 综合 AQI，AQI 是标签，缺了这行就没法训练。"""
    out, dropped = [], {}
    index: dict[tuple[str, str], int] = {}
    for row in rows:
        site = (row.get("SITE") or "").strip().lower()
        date = parse_date(row.get("DATE"))
        if not site or date is None:
            dropped["bad_key"] = dropped.get("bad_key", 0) + 1
            continue
        aqi = parse_number(row.get("AQI"))
        if aqi is None or not (AQI_RANGE[0] <= aqi <= AQI_RANGE[1]):
            dropped["aqi_missing_or_invalid"] = dropped.get("aqi_missing_or_invalid", 0) + 1
            continue
        record = {
            "SITE": site,
            "STATION": (row.get("STATION") or "").strip(),
            "DATE": date,
            "NAME": (row.get("NAME") or "").strip(),
            "AQI": aqi,
        }
        for column, (low, high) in WEATHER_RANGE.items():
            value = parse_number(row.get(column))
            if value is not None and not (low <= value <= high):
                dropped["weather_out_of_range"] = dropped.get("weather_out_of_range", 0) + 1
                value = None
            record[column] = value
        month = parse_number(row.get("MONTH"))
        record["MONTH"] = int(month) if month is not None and 1 <= month <= 12 else int(date[5:7])
        key = (site, date)
        previous = index.get(key)
        if previous is None:
            index[key] = len(out)
            out.append(record)
        else:
            dropped["duplicate_site_day"] = dropped.get("duplicate_site_day", 0) + 1
            out[previous] = record
    return out, dropped, ["SITE", "STATION", "DATE", "NAME", "TEMP", "DEWP", "STP", "VISIB", "WDSP", "MXSPD", "MAX", "MIN", "PRCP", "MONTH", "AQI"]


CLEANERS = {
    "openaq": ("day", clean_openaq, ["day", "average", "coverage", "unit", "parameter"]),
    "integrated": ("datetime", clean_integrated, ["datetime", *POLLUTANTS]),
    "aqi": ("datetime", clean_aqi, None),
    "merged": ("DATE", clean_merged, None),
}


def clean_file(input_path: Path, output_path: Path, kind: str = "auto", min_coverage: float = 0.0) -> dict:
    """清洗一个 CSV，写出结果并返回报告（报告也用于测试断言）。"""
    try:
        with input_path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            fieldnames = [name for name in (reader.fieldnames or []) if name]
            rows = list(reader)
    except OSError as exc:
        raise CleanError(f"读不了输入文件 {input_path}: {exc}") from exc

    if not fieldnames:
        raise CleanError(f"输入没有表头：{input_path}")
    resolved = kind if kind != "auto" else detect_kind(fieldnames)
    if resolved not in CLEANERS:
        raise CleanError(f"未知 --kind={kind}，可选：auto / {' / '.join(KINDS)}")

    _key, cleaner, columns = CLEANERS[resolved]
    result = cleaner(rows, min_coverage)
    if len(result) == 2:
        cleaned, dropped = result
    else:
        cleaned, dropped = result[0], result[1]
        columns = result[2]

    # 列顺序固定，别让不同批次的输出难以 diff。
    ordered = [{column: record.get(column) for column in columns} for record in cleaned]
    ordered.sort(key=lambda item: (item.get(columns[0]) or ""))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for record in ordered:
            writer.writerow({column: "" if record[column] is None else record[column] for column in columns})

    report = {
        "kind": resolved,
        "input": str(input_path),
        "output": str(output_path),
        "rows_in": len(rows),
        "rows_out": len(ordered),
        "dropped": dict(sorted(dropped.items())),
        "dropped_total": sum(dropped.values()),
        "null_counts": {
            column: sum(1 for record in ordered if record[column] is None) for column in columns
        },
        "min_coverage": min_coverage,
    }
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="清洗 AQI 流水线各阶段的 CSV")
    parser.add_argument("--input", required=True, type=Path, help="待清洗的 CSV")
    parser.add_argument("--output", required=True, type=Path, help="清洗后的 CSV")
    parser.add_argument("--kind", default="auto", choices=("auto", *KINDS), help="数据形态，默认按表头自动识别")
    parser.add_argument("--min-coverage", type=float, default=0.0, help="openaq：覆盖率低于该值的行直接丢弃（默认 0 = 不过滤）")
    parser.add_argument("--report", type=Path, default=None, help="把清洗报告写成 JSON")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        report = clean_file(args.input, args.output, kind=args.kind, min_coverage=args.min_coverage)
    except CleanError as exc:
        print(f"清洗失败：{exc}", file=sys.stderr)
        return 2
    text = json.dumps(report, ensure_ascii=False, indent=2)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
