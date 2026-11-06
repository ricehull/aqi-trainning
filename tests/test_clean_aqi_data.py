"""clean_aqi_data.py 鐨勫崟鍏冩祴璇曘€?
璺戞硶锛堜粨搴撴牴鐩綍锛夛細
    python -m unittest discover -s tests -v

涓嶄緷璧?pandas / autogluon / 缃戠粶锛氬叏閮ㄧ敤涓存椂鐩綍閲岀殑鍚堟垚 CSV銆?"""

from __future__ import annotations

import csv
import io
import json
import shutil
import sys
import tempfile
import unittest
import uuid
from contextlib import contextmanager, redirect_stdout
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import clean_aqi_data as cleaner  # noqa: E402

BASE = Path(__file__).resolve().parent.parent / ".tmp"
BASE.mkdir(parents=True, exist_ok=True)


@contextmanager
def workdir():
    """临时目录放在仓库内：沙盒允许写工作区，但不允许清理系统临时目录。"""
    path = BASE / ("aqi-" + uuid.uuid4().hex[:8])
    path.mkdir(parents=True, exist_ok=True)
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)



def write_csv(path: Path, fieldnames: list[str], rows: list[list[str]]) -> Path:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(fieldnames)
        writer.writerows(rows)
    return path


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


class DetectKindTest(unittest.TestCase):
    def test_detects_each_shape(self):
        cases = {
            "openaq": ["day", "average", "coverage", "unit", "parameter"],
            "integrated": ["datetime", "pm25", "pm10", "o3", "co", "so2", "no2"],
            "aqi": ["datetime", "pm25", "pm25_aqi", "overall_aqi"],
            "merged": ["SITE", "STATION", "DATE", "NAME", "TEMP", "AQI"],
        }
        for expected, fields in cases.items():
            with self.subTest(expected=expected):
                self.assertEqual(cleaner.detect_kind(fields), expected)

    def test_unknown_header_raises(self):
        with self.assertRaises(cleaner.CleanError):
            cleaner.detect_kind(["foo", "bar"])


class ParseHelpersTest(unittest.TestCase):
    def test_parse_date_accepts_common_forms(self):
        self.assertEqual(cleaner.parse_date("2024-01-02"), "2024-01-02")
        self.assertEqual(cleaner.parse_date("2024-1-2"), "2024-01-02")
        self.assertEqual(cleaner.parse_date("2024/01/02"), "2024-01-02")
        self.assertEqual(cleaner.parse_date("20240102"), "2024-01-02")
        self.assertEqual(cleaner.parse_date("2024-01-01 13:00:00"), "2024-01-01")

    def test_parse_date_rejects_garbage(self):
        for raw in ("", None, "not-a-date", "2024-13-01"):
            with self.subTest(raw=raw):
                self.assertIsNone(cleaner.parse_date(raw))

    def test_parse_number_handles_placeholders(self):
        self.assertEqual(cleaner.parse_number(" 3.5 "), 3.5)
        self.assertIsNone(cleaner.parse_number(""))
        self.assertIsNone(cleaner.parse_number("NA"))
        self.assertIsNone(cleaner.parse_number("NaN"))
        self.assertIsNone(cleaner.parse_number("abc"))


class OpenAQCleanTest(unittest.TestCase):
    def test_drops_negative_and_missing_and_reports_counts(self):
        with workdir() as tmp:
            tmp_path = Path(tmp)
            source = write_csv(
                tmp_path / "pm25.csv",
                ["day", "average", "coverage", "unit", "parameter"],
                [
                    ["2024-03-25", "3.72", "21.0", "碌g/m鲁", "pm25"],
                    ["2024-03-26", "4.6", "100.0", "碌g/m鲁", "pm25"],
                    ["2024-03-30", "-0.62", "83.0", "碌g/m鲁", "pm25"],
                    ["2024-03-31", "", "75.0", "碌g/m鲁", "pm25"],
                    ["bad-date", "1.0", "100.0", "碌g/m鲁", "pm25"],
                ],
            )
            report = cleaner.clean_file(source, tmp_path / "out.csv")
            rows = read_csv(tmp_path / "out.csv")

        self.assertEqual(report["kind"], "openaq")
        self.assertEqual(report["rows_in"], 5)
        self.assertEqual(report["rows_out"], 2)
        self.assertEqual(report["dropped"]["negative_value"], 1)
        self.assertEqual(report["dropped"]["missing_value"], 1)
        self.assertEqual(report["dropped"]["bad_date"], 1)
        self.assertEqual([row["day"] for row in rows], ["2024-03-25", "2024-03-26"])

    def test_duplicate_day_keeps_higher_coverage(self):
        with workdir() as tmp:
            tmp_path = Path(tmp)
            source = write_csv(
                tmp_path / "pm25.csv",
                ["day", "average", "coverage", "unit", "parameter"],
                [
                    ["2024-01-01", "5.0", "20.0", "碌g/m鲁", "pm25"],
                    ["2024-01-01", "6.0", "90.0", "碌g/m鲁", "pm25"],
                ],
            )
            report = cleaner.clean_file(source, tmp_path / "out.csv")
            rows = read_csv(tmp_path / "out.csv")

        self.assertEqual(report["rows_out"], 1)
        self.assertEqual(report["dropped"]["duplicate_day"], 1)
        self.assertEqual(rows[0]["average"], "6.0")

    def test_min_coverage_filters_low_confidence_rows(self):
        with workdir() as tmp:
            tmp_path = Path(tmp)
            source = write_csv(
                tmp_path / "pm25.csv",
                ["day", "average", "coverage", "unit", "parameter"],
                [
                    ["2024-01-01", "5.0", "20.0", "碌g/m鲁", "pm25"],
                    ["2024-01-02", "6.0", "90.0", "碌g/m鲁", "pm25"],
                ],
            )
            report = cleaner.clean_file(source, tmp_path / "out.csv", min_coverage=75)
            rows = read_csv(tmp_path / "out.csv")

        self.assertEqual(report["rows_out"], 1)
        self.assertEqual(report["dropped"]["low_coverage"], 1)
        self.assertEqual(rows[0]["day"], "2024-01-02")

    def test_above_physical_cap_is_rejected(self):
        with workdir() as tmp:
            tmp_path = Path(tmp)
            source = write_csv(
                tmp_path / "pm25.csv",
                ["day", "average", "coverage", "unit", "parameter"],
                [["2024-01-01", "9999", "100.0", "碌g/m鲁", "pm25"]],
            )
            report = cleaner.clean_file(source, tmp_path / "out.csv")

        self.assertEqual(report["rows_out"], 0)
        self.assertEqual(report["dropped"]["above_physical_cap"], 1)


class IntegratedCleanTest(unittest.TestCase):
    def test_negative_becomes_null_and_empty_rows_dropped(self):
        with workdir() as tmp:
            tmp_path = Path(tmp)
            source = write_csv(
                tmp_path / "integrated.csv",
                ["datetime", "pm25", "pm10", "o3", "co", "so2", "no2"],
                [
                    ["2024-01-01", "", "18.3", "0.02", "0.2", "", "0.0035"],
                    ["2024-01-02", "-1.0", "", "", "", "", ""],
                    ["2024-01-03", "", "", "", "", "", ""],
                ],
            )
            report = cleaner.clean_file(source, tmp_path / "out.csv")
            rows = read_csv(tmp_path / "out.csv")

        self.assertEqual(report["rows_out"], 1)
        self.assertEqual(report["dropped"]["invalid_concentration"], 1)
        self.assertEqual(report["dropped"]["empty_row"], 2)
        self.assertEqual(rows[0]["pm25"], "")


class AQICleanTest(unittest.TestCase):
    def test_out_of_range_aqi_is_nulled_but_row_kept(self):
        with workdir() as tmp:
            tmp_path = Path(tmp)
            source = write_csv(
                tmp_path / "aqi.csv",
                ["datetime", "pm25", "pm25_aqi", "overall_aqi"],
                [
                    ["2024-01-01", "12.0", "999", "12.0"],
                    ["2024-01-02", "", "", ""],
                ],
            )
            report = cleaner.clean_file(source, tmp_path / "out.csv")
            rows = read_csv(tmp_path / "out.csv")

        self.assertEqual(report["rows_out"], 1)
        self.assertEqual(report["dropped"]["aqi_out_of_range"], 1)
        self.assertEqual(report["dropped"]["empty_row"], 1)
        self.assertEqual(rows[0]["pm25_aqi"], "")
        self.assertEqual(rows[0]["overall_aqi"], "12.0")


class MergedCleanTest(unittest.TestCase):
    def test_requires_aqi_and_normalizes_weather(self):
        with workdir() as tmp:
            tmp_path = Path(tmp)
            source = write_csv(
                tmp_path / "merged.csv",
                ["SITE", "STATION", "DATE", "NAME", "TEMP", "AQI"],
                [
                    ["bakersfield", "72278023183", "2024-01-01", '"PHOENIX AIRPORT, AZ US"', "54.3", "19.0"],
                    ["bakersfield", "72278023183", "2024-01-02", "X", "999", ""],
                    ["bakersfield", "72278023183", "2024-01-01", "Y", "50.0", "20.0"],
                ],
            )
            report = cleaner.clean_file(source, tmp_path / "out.csv", kind="merged")
            rows = read_csv(tmp_path / "out.csv")

        self.assertEqual(report["kind"], "merged")
        self.assertEqual(report["rows_out"], 1)
        self.assertEqual(report["dropped"]["aqi_missing_or_invalid"], 1)
        self.assertEqual(report["dropped"]["duplicate_site_day"], 1)
        self.assertEqual(rows[0]["AQI"], "20.0")
        self.assertEqual(rows[0]["MONTH"], "1")

    def test_weather_out_of_range_becomes_null(self):
        with workdir() as tmp:
            tmp_path = Path(tmp)
            source = write_csv(
                tmp_path / "merged.csv",
                ["SITE", "STATION", "DATE", "NAME", "TEMP", "AQI"],
                [["fresno", "1", "2024-01-01", "F", "999", "30.0"]],
            )
            report = cleaner.clean_file(source, tmp_path / "out.csv", kind="merged")
            rows = read_csv(tmp_path / "out.csv")

        self.assertEqual(report["dropped"]["weather_out_of_range"], 1)
        self.assertEqual(rows[0]["TEMP"], "")


class CliTest(unittest.TestCase):
    def test_cli_writes_output_and_report(self):
        with workdir() as tmp:
            tmp_path = Path(tmp)
            source = write_csv(
                tmp_path / "pm25.csv",
                ["day", "average", "coverage", "unit", "parameter"],
                [["2024-01-01", "5.0", "100.0", "碌g/m鲁", "pm25"]],
            )
            buffer = io.StringIO()
            with redirect_stdout(buffer):
                code = cleaner.main(
                    [
                        "--input",
                        str(source),
                        "--output",
                        str(tmp_path / "out.csv"),
                        "--report",
                        str(tmp_path / "report.json"),
                    ]
                )
            report = json.loads((tmp_path / "report.json").read_text(encoding="utf-8"))

        self.assertEqual(code, 0)
        self.assertEqual(report["rows_out"], 1)
        self.assertEqual(json.loads(buffer.getvalue())["rows_in"], 1)

    def test_cli_returns_2_on_unreadable_input(self):
        buffer = io.StringIO()
        with redirect_stdout(io.StringIO()), workdir() as tmp:
            code = cleaner.main(
                ["--input", str(Path(tmp) / "missing.csv"), "--output", str(Path(tmp) / "out.csv")]
            )
        self.assertEqual(code, 2)
        self.assertEqual(buffer.getvalue(), "")

    def test_cleaning_twice_is_stable(self):
        with workdir() as tmp:
            tmp_path = Path(tmp)
            source = write_csv(
                tmp_path / "pm25.csv",
                ["day", "average", "coverage", "unit", "parameter"],
                [
                    ["2024-01-02", "6.0", "90.0", "碌g/m鲁", "pm25"],
                    ["2024-01-01", "5.0", "100.0", "碌g/m鲁", "pm25"],
                ],
            )
            first = cleaner.clean_file(source, tmp_path / "once.csv", kind="openaq")
            second = cleaner.clean_file(tmp_path / "once.csv", tmp_path / "twice.csv", kind="openaq")
            once_rows = read_csv(tmp_path / "once.csv")
            twice_rows = read_csv(tmp_path / "twice.csv")

        self.assertEqual(first["rows_out"], 2)
        self.assertEqual(second["rows_out"], 2)
        self.assertEqual(once_rows, twice_rows)


if __name__ == "__main__":
    unittest.main()






