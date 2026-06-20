from __future__ import annotations

from datetime import datetime
import math
import re
import statistics
import unicodedata

from .models import (
    DataQualityIssue,
    DataQualityReport,
    DatasetColumn,
    DatasetProfile,
    TabularDataset,
)


class DatasetProfiler:
    version = "1.0"

    def profile(self, dataset: TabularDataset) -> DatasetProfile:
        missing_counts = {column: 0 for column in dataset.columns}
        numeric_values: dict[str, list[float]] = {column: [] for column in dataset.columns}
        seen_rows: set[tuple[tuple[str, str], ...]] = set()
        duplicate_count = 0
        unique_values: dict[str, set[str]] = {column: set() for column in dataset.columns}
        type_hits = {
            column: {"number": 0, "integer": 0, "datetime": 0, "boolean": 0, "string": 0}
            for column in dataset.columns
        }
        datetime_values: dict[str, list[str]] = {column: [] for column in dataset.columns}
        currency_hits: dict[str, dict[str, int]] = {column: {} for column in dataset.columns}
        unit_hits: dict[str, dict[str, int]] = {column: {} for column in dataset.columns}
        parsing_warnings = list(
            dataset.lineage.get("table_metadata", {}).get("warnings", [])
            if isinstance(dataset.lineage.get("table_metadata"), dict)
            else []
        )
        metadata = dataset.lineage.get("table_metadata")
        if isinstance(metadata, dict):
            for key in ("encoding", "delimiter", "truncated_for_analysis"):
                if key in metadata:
                    parsing_warnings.append(f"{key}={metadata[key]}")

        for row in dataset.rows:
            fingerprint = tuple(sorted((str(k), str(v)) for k, v in row.items()))
            if fingerprint in seen_rows:
                duplicate_count += 1
            seen_rows.add(fingerprint)
            for column in dataset.columns:
                value = row.get(column)
                if value is None or value == "":
                    missing_counts[column] += 1
                    continue
                text = str(value).strip()
                unique_values[column].add(text)
                currency = self._detect_currency(text) or self._detect_currency(column)
                if currency:
                    currency_hits[column][currency] = currency_hits[column].get(currency, 0) + 1
                unit = self._detect_unit(text) or self._detect_unit(column)
                if unit:
                    unit_hits[column][unit] = unit_hits[column].get(unit, 0) + 1
                boolean = text.lower() in {"true", "false", "yes", "no", "是", "否"}
                if boolean:
                    type_hits[column]["boolean"] += 1
                    continue
                number = self._parse_number(text)
                if number is not None:
                    type_hits[column]["number"] += 1
                    if float(number).is_integer():
                        type_hits[column]["integer"] += 1
                    numeric_values[column].append(number)
                    continue
                parsed_datetime = self._parse_datetime(text)
                if parsed_datetime:
                    type_hits[column]["datetime"] += 1
                    datetime_values[column].append(parsed_datetime)
                    continue
                type_hits[column]["string"] += 1

        row_count = len(dataset.rows)
        missing_rates = {
            column: (count / row_count if row_count else 0.0)
            for column, count in missing_counts.items()
        }
        unique_counts = {column: len(values) for column, values in unique_values.items()}
        numeric_columns = [
            column
            for column, values in numeric_values.items()
            if row_count and len(values) / row_count >= 0.8
        ]
        likely_time_columns = [
            column
            for column, values in datetime_values.items()
            if row_count and len(values) / row_count >= 0.8
        ]
        likely_primary_keys = [
            column
            for column, count in unique_counts.items()
            if row_count
            and count == row_count
            and missing_counts[column] == 0
            and column not in likely_time_columns
            and (column.lower() in {"id", "uuid"} or column not in numeric_columns)
        ]
        likely_category_columns = [
            column
            for column, count in unique_counts.items()
            if row_count and 1 < count <= max(20, row_count // 2)
        ]
        currencies = {
            column: self._most_common(values)
            for column, values in currency_hits.items()
            if values
        }
        units = {
            column: self._most_common(values)
            for column, values in unit_hits.items()
            if values
        }
        schema = [
            DatasetColumn(
                name=column,
                normalized_name=self._normalize_column_name(column),
                inferred_type=self._infer_type(
                    type_hits[column],
                    row_count - missing_counts[column],
                    column in numeric_columns,
                    column in likely_time_columns,
                ),
                nullable=missing_counts[column] > 0,
                unit=units.get(column),
                currency=currencies.get(column),
                source_column=column,
            )
            for column in dataset.columns
        ]
        quantiles = {
            column: self._quantiles(values)
            for column, values in numeric_values.items()
            if values
        }
        warnings: list[str] = []
        if duplicate_count:
            warnings.append(f"duplicate_rows={duplicate_count}")
        sparse_columns = [
            column
            for column, count in missing_counts.items()
            if row_count and count / row_count > 0.5
        ]
        if sparse_columns:
            warnings.append("sparse_columns=" + ",".join(sparse_columns))
        outliers = {
            column: self._outliers(dataset.rows, column, values)
            for column, values in numeric_values.items()
            if len(values) >= 4
        }
        outliers = {column: values for column, values in outliers.items() if values}
        if outliers:
            warnings.append(
                "outlier_candidates=" + ",".join(sorted(outliers.keys()))
            )
        mixed_columns = [
            column
            for column, hits in type_hits.items()
            if self._infer_type(
                hits,
                row_count - missing_counts[column],
                column in numeric_columns,
                column in likely_time_columns,
            )
            == "mixed"
        ]
        if mixed_columns:
            warnings.append("mixed_type_columns=" + ",".join(mixed_columns))

        return DatasetProfile(
            dataset_id=dataset.dataset_id,
            row_count=row_count,
            column_count=len(dataset.columns),
            missing_counts=missing_counts,
            duplicate_row_count=duplicate_count,
            numeric_columns=numeric_columns,
            quality_warnings=warnings,
            schema=schema,
            missing_rates=missing_rates,
            duplicate_row_rate=duplicate_count / row_count if row_count else 0.0,
            unique_counts=unique_counts,
            min_values={
                column: min(values)
                for column, values in numeric_values.items()
                if values
            },
            max_values={
                column: max(values)
                for column, values in numeric_values.items()
                if values
            },
            mean_values={
                column: sum(values) / len(values)
                for column, values in numeric_values.items()
                if values
            },
            median_values={
                column: statistics.median(values)
                for column, values in numeric_values.items()
                if values
            },
            quantiles=quantiles,
            time_ranges={
                column: {"min": min(values), "max": max(values)}
                for column, values in datetime_values.items()
                if values
            },
            negative_ratios={
                column: sum(1 for value in values if value < 0) / len(values)
                for column, values in numeric_values.items()
                if values
            },
            outlier_candidates=outliers,
            currencies=currencies,
            units=units,
            likely_primary_keys=likely_primary_keys,
            likely_category_columns=likely_category_columns,
            likely_time_columns=likely_time_columns,
            parsing_warnings=parsing_warnings,
        )

    def quality_report(self, profile: DatasetProfile) -> DataQualityReport:
        issues: list[DataQualityIssue] = []
        if profile.row_count == 0:
            issues.append(DataQualityIssue("empty_dataset", "high", "数据集为空"))
        if profile.duplicate_row_count:
            issues.append(
                DataQualityIssue(
                    "duplicate_rows",
                    "medium",
                    f"存在 {profile.duplicate_row_count} 行重复数据",
                )
            )
        for column, rate in profile.missing_rates.items():
            if rate > 0.5:
                issues.append(
                    DataQualityIssue(
                        "high_missing_rate",
                        "high",
                        f"字段缺失率 {rate:.0%}",
                        column=column,
                    )
                )
            elif rate > 0.1:
                issues.append(
                    DataQualityIssue(
                        "medium_missing_rate",
                        "medium",
                        f"字段缺失率 {rate:.0%}",
                        column=column,
                    )
                )
        for column, candidates in profile.outlier_candidates.items():
            if candidates:
                issues.append(
                    DataQualityIssue(
                        "outlier_candidates",
                        "medium",
                        f"发现 {len(candidates)} 个异常值候选",
                        column=column,
                    )
                )
        high_count = sum(1 for issue in issues if issue.severity == "high")
        medium_count = sum(1 for issue in issues if issue.severity == "medium")
        if profile.row_count == 0 or high_count >= 2:
            level = "unusable"
        elif high_count:
            level = "low"
        elif medium_count:
            level = "medium"
        else:
            level = "high"
        usable_for = ["summary"]
        not_usable_for: list[str] = []
        if level in {"high", "medium"} and profile.numeric_columns:
            usable_for.extend(["pricing", "trend", "comparison"])
        else:
            not_usable_for.extend(["pricing", "trend", "comparison"])
        return DataQualityReport(
            dataset_id=profile.dataset_id,
            quality_level=level,
            issues=issues,
            usable_for=usable_for,
            not_usable_for=not_usable_for,
        )

    @staticmethod
    def _parse_number(value: str) -> float | None:
        cleaned = value.strip()
        cleaned = re.sub(r"^[¥$€£]\s*", "", cleaned)
        cleaned = re.sub(r"\s*(CNY|RMB|USD|EUR|GBP|元|美元)$", "", cleaned, flags=re.I)
        cleaned = re.sub(r"\s*(ms|sec|s|min|h|gb|mb|kg|台|件)$", "", cleaned, flags=re.I)
        cleaned = cleaned.replace(",", "")
        if cleaned.endswith("%"):
            cleaned = cleaned[:-1]
        try:
            number = float(cleaned)
        except ValueError:
            return None
        if math.isfinite(number):
            return number
        return None

    @staticmethod
    def _parse_datetime(value: str) -> str | None:
        text = value.strip()
        if not text:
            return None
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y-%m", "%Y/%m", "%Y"):
            try:
                return datetime.strptime(text, fmt).date().isoformat()
            except ValueError:
                continue
        return None

    @classmethod
    def _infer_type(
        cls,
        hits: dict[str, int],
        present_count: int,
        numeric: bool,
        temporal: bool,
    ) -> str:
        if present_count == 0:
            return "empty"
        if temporal:
            return "datetime"
        if numeric:
            if hits["integer"] == hits["number"]:
                return "integer"
            return "number"
        if hits["boolean"] / present_count >= 0.8:
            return "boolean"
        nonzero = sum(1 for value in hits.values() if value)
        return "string" if nonzero <= 1 or hits["string"] / present_count >= 0.8 else "mixed"

    @staticmethod
    def _normalize_column_name(value: str) -> str:
        normalized = unicodedata.normalize("NFKC", value).strip().lower()
        normalized = re.sub(r"[^0-9a-zA-Z\u4e00-\u9fff]+", "_", normalized)
        return normalized.strip("_") or "column"

    @staticmethod
    def _quantiles(values: list[float]) -> dict[str, float]:
        ordered = sorted(values)
        if len(ordered) == 1:
            return {"p25": ordered[0], "p50": ordered[0], "p75": ordered[0]}
        return {
            "p25": DatasetProfiler._percentile(ordered, 0.25),
            "p50": DatasetProfiler._percentile(ordered, 0.50),
            "p75": DatasetProfiler._percentile(ordered, 0.75),
        }

    @staticmethod
    def _percentile(ordered: list[float], fraction: float) -> float:
        position = (len(ordered) - 1) * fraction
        lower = math.floor(position)
        upper = math.ceil(position)
        if lower == upper:
            return ordered[int(position)]
        return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)

    @staticmethod
    def _outliers(
        rows: list[dict[str, object]], column: str, values: list[float]
    ) -> list[dict[str, object]]:
        quantiles = DatasetProfiler._quantiles(values)
        iqr = quantiles["p75"] - quantiles["p25"]
        if iqr <= 0:
            return []
        low = quantiles["p25"] - 1.5 * iqr
        high = quantiles["p75"] + 1.5 * iqr
        candidates: list[dict[str, object]] = []
        for index, row in enumerate(rows, start=1):
            number = DatasetProfiler._parse_number(str(row.get(column, "")))
            if number is None:
                continue
            if number < low or number > high:
                candidates.append({"row_index": index, "value": number, "bounds": [low, high]})
            if len(candidates) >= 20:
                break
        return candidates

    @staticmethod
    def _detect_currency(value: str) -> str | None:
        text = value.upper()
        if "¥" in value or "元" in value or "人民币" in value or "RMB" in text or "CNY" in text:
            return "CNY"
        if "$" in value or "USD" in text or "美元" in value:
            return "USD"
        if "€" in value or "EUR" in text:
            return "EUR"
        if "£" in value or "GBP" in text:
            return "GBP"
        return None

    @staticmethod
    def _detect_unit(value: str) -> str | None:
        text = value.lower()
        patterns = [
            ("percent", r"%|百分比|增长率|率$"),
            ("ms", r"(^|[_\W])ms($|[_\W])|毫秒"),
            ("s", r"\bsec\b|\bs\b|秒"),
            ("minute", r"\bmin\b|分钟"),
            ("hour", r"\bh\b|小时"),
            ("GB", r"\bgb\b"),
            ("MB", r"\bmb\b"),
            ("kg", r"\bkg\b|千克|公斤"),
            ("unit", r"数量|销量|台|件"),
        ]
        for unit, pattern in patterns:
            if re.search(pattern, text):
                return unit
        return None

    @staticmethod
    def _most_common(values: dict[str, int]) -> str:
        return sorted(values.items(), key=lambda item: (-item[1], item[0]))[0][0]
