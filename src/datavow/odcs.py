"""ODCS v3.1 JSON Schema validation and format adapter.

Provides:
  - validate_odcs_schema(): Validate a YAML file against the official ODCS JSON Schema.
  - detect_format(): Auto-detect DataVow vs ODCS-native format.
  - odcs_to_datavow(): Convert an ODCS-native contract to DataVow internal model.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

try:
    from jsonschema import Draft201909Validator, ValidationError
except ImportError:
    raise ImportError(
        "jsonschema is required for ODCS validation. Install with: pip install jsonschema"
    )


# ── Schema loading ────────────────────────────────────────────

_SCHEMA_CACHE: dict[str, Any] = {}


def _load_odcs_schema(version: str = "v3.1.0") -> dict[str, Any]:
    """Load the bundled ODCS JSON Schema."""
    if version in _SCHEMA_CACHE:
        return _SCHEMA_CACHE[version]

    schema_file = Path(__file__).parent / "schemas" / f"odcs-json-schema-{version}.json"
    if not schema_file.exists():
        raise FileNotFoundError(
            f"ODCS JSON Schema {version} not found. "
            f"Available: {', '.join(p.stem for p in schema_file.parent.glob('*.json'))}"
        )

    with open(schema_file) as f:
        schema = json.load(f)

    _SCHEMA_CACHE[version] = schema
    return schema


# ── Format detection ──────────────────────────────────────────


class ContractFormat:
    DATAVOW = "datavow"
    ODCS = "odcs"
    UNKNOWN = "unknown"


def detect_format(raw: dict[str, Any]) -> str:
    """Detect whether a parsed YAML dict is DataVow-native or ODCS-native.

    DataVow format indicators:
      - apiVersion starts with "datavow/"
      - Has a 'metadata' block

    ODCS format indicators:
      - apiVersion matches v3.x.x or v2.x.x
      - Top-level 'id' and 'status' fields
      - No 'metadata' block
    """
    api_version = raw.get("apiVersion", "")

    if api_version.startswith("datavow/") or "metadata" in raw:
        return ContractFormat.DATAVOW

    if api_version.startswith("v3.") or api_version.startswith("v2."):
        return ContractFormat.ODCS

    # Heuristic: if top-level has 'id' + 'status' + 'schema' as array → ODCS
    if "id" in raw and "status" in raw and isinstance(raw.get("schema"), list):
        return ContractFormat.ODCS

    return ContractFormat.UNKNOWN


# ── ODCS JSON Schema validation ───────────────────────────────


@dataclass
class OdcsValidationError:
    """Single ODCS schema validation error."""

    path: str
    message: str
    value: Any = None


@dataclass
class OdcsValidationResult:
    """Result of validating a contract against the ODCS JSON Schema."""

    valid: bool
    errors: list[OdcsValidationError] = field(default_factory=list)
    schema_version: str = "v3.1.0"

    @property
    def error_count(self) -> int:
        return len(self.errors)

    def summary(self) -> str:
        if self.valid:
            return f"ODCS {self.schema_version} schema: valid"
        return f"ODCS {self.schema_version} schema: {self.error_count} error(s)"


def validate_odcs_schema(
    path: str | Path,
    schema_version: str = "v3.1.0",
) -> OdcsValidationResult:
    """Validate a YAML file against the official ODCS JSON Schema."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Contract file not found: {path}")

    with open(path) as f:
        raw = yaml.safe_load(f)

    if not isinstance(raw, dict):
        return OdcsValidationResult(
            valid=False,
            errors=[OdcsValidationError(path="$", message="Expected a YAML mapping at root")],
            schema_version=schema_version,
        )

    return validate_odcs_schema_dict(raw, schema_version=schema_version)


def validate_odcs_schema_dict(
    raw: dict[str, Any],
    schema_version: str = "v3.1.0",
) -> OdcsValidationResult:
    """Validate a parsed dict against the official ODCS JSON Schema."""
    schema = _load_odcs_schema(schema_version)
    validator = Draft201909Validator(schema)

    errors: list[OdcsValidationError] = []
    for error in sorted(validator.iter_errors(raw), key=lambda e: list(e.path)):
        json_path = ".".join(str(p) for p in error.absolute_path) or "$"
        errors.append(
            OdcsValidationError(
                path=json_path,
                message=error.message,
                value=error.instance if not isinstance(error.instance, (dict, list)) else None,
            )
        )

    return OdcsValidationResult(
        valid=len(errors) == 0,
        errors=errors,
        schema_version=schema_version,
    )


# ── ODCS → DataVow adapter ───────────────────────────────────

_ODCS_TYPE_MAP: dict[str, str] = {
    "string": "string",
    "number": "float",
    "integer": "integer",
    "boolean": "boolean",
    "date": "date",
    "timestamp": "timestamp",
    "time": "timestamp",
    "object": "string",
    "array": "string",
}

_ODCS_SEVERITY_MAP: dict[str, str] = {
    "error": "CRITICAL",
    "warning": "WARNING",
    "info": "INFO",
    "CRITICAL": "CRITICAL",
    "WARNING": "WARNING",
    "INFO": "INFO",
}


def odcs_to_datavow(raw: dict[str, Any]) -> dict[str, Any]:
    """Convert an ODCS-native contract dict to DataVow's internal format."""
    # Metadata
    name = raw.get("name", raw.get("dataProduct", raw.get("id", "unnamed")))
    version = raw.get("version", "1.0.0")
    domain = raw.get("domain", "")
    description = ""
    desc_block = raw.get("description", {})
    if isinstance(desc_block, dict):
        description = desc_block.get("purpose", desc_block.get("usage", ""))
    elif isinstance(desc_block, str):
        description = desc_block

    # Tags
    tags = []
    raw_tags = raw.get("tags", [])
    if isinstance(raw_tags, list):
        for t in raw_tags:
            if isinstance(t, dict):
                tags.append(t.get("name", str(t)))
            else:
                tags.append(str(t))

    # Owner from team block
    owner = ""
    team = raw.get("team", {})
    if isinstance(team, dict):
        for role_key in ("productOwner", "dataOwner", "technicalOwner"):
            members = team.get(role_key, [])
            if isinstance(members, list) and members:
                first = members[0]
                if isinstance(first, dict):
                    owner = first.get("name", first.get("email", ""))
                elif isinstance(first, str):
                    owner = first
                if owner:
                    break
    elif isinstance(team, list):
        if team:
            first = team[0]
            if isinstance(first, dict):
                owner = first.get("name", first.get("email", ""))

    # Schema
    fields: list[dict[str, Any]] = []
    quality_rules: list[dict[str, Any]] = []

    schema_list = raw.get("schema", [])
    if isinstance(schema_list, list):
        for schema_obj in schema_list:
            if not isinstance(schema_obj, dict):
                continue
            props = schema_obj.get("properties", [])
            if isinstance(props, list):
                for prop in props:
                    if not isinstance(prop, dict):
                        continue
                    field_def = _convert_property(prop)
                    fields.append(field_def)
                    prop_quality = prop.get("quality", [])
                    if isinstance(prop_quality, list):
                        for qr in prop_quality:
                            rule = _convert_quality_rule(qr, field_name=prop.get("name", ""))
                            if rule:
                                quality_rules.append(rule)

            schema_quality = schema_obj.get("quality", [])
            if isinstance(schema_quality, list):
                for qr in schema_quality:
                    rule = _convert_quality_rule(qr)
                    if rule:
                        quality_rules.append(rule)

    # SLA
    sla: dict[str, Any] = {}
    sla_props = raw.get("slaProperties", [])
    if isinstance(sla_props, list):
        for sp in sla_props:
            if not isinstance(sp, dict):
                continue
            prop_name = sp.get("property", "").lower()
            value = sp.get("value", "")
            if "freshness" in prop_name or "latency" in prop_name:
                sla["freshness"] = str(value)
            elif "completeness" in prop_name:
                sla["completeness"] = str(value)
            elif "availability" in prop_name:
                sla["availability"] = str(value)

    result: dict[str, Any] = {
        "apiVersion": "datavow/v1",
        "kind": "DataContract",
        "metadata": {
            "name": name,
            "version": version,
            "owner": owner,
            "domain": domain,
            "description": description,
            "tags": tags,
        },
        "schema": {
            "type": "table",
            "fields": fields if fields else [{"name": "_placeholder", "type": "string"}],
        },
    }

    if quality_rules:
        result["quality"] = {"rules": quality_rules}
    if sla:
        result["sla"] = sla

    return result


def _convert_property(prop: dict[str, Any]) -> dict[str, Any]:
    """Convert an ODCS SchemaProperty to a DataVow FieldSpec dict."""
    logical_type = prop.get("logicalType", "string")
    datavow_type = _ODCS_TYPE_MAP.get(str(logical_type).lower(), "string")

    field_def: dict[str, Any] = {
        "name": prop.get("name", "unknown"),
        "type": datavow_type,
        "required": bool(prop.get("required", False)),
        "unique": bool(prop.get("unique", False)),
    }

    classification = str(prop.get("classification", "")).lower()
    if classification in ("confidential", "restricted", "pii", "sensitive"):
        field_def["pii"] = True

    desc = prop.get("description", prop.get("businessName", ""))
    if desc:
        field_def["description"] = desc

    return field_def


def _convert_quality_rule(
    qr: dict[str, Any], field_name: str = ""
) -> dict[str, Any] | None:
    """Convert an ODCS DataQuality rule to a DataVow QualityRule dict."""
    rule_type = qr.get("type", "")
    rule_name = qr.get("name", qr.get("description", f"rule_{id(qr)}"))
    severity = _ODCS_SEVERITY_MAP.get(str(qr.get("severity", "warning")), "WARNING")

    if rule_type == "sql":
        query = qr.get("query", "")
        if not query:
            return None
        threshold = qr.get("mustBe", qr.get("mustNotBe", 0))
        return {
            "name": rule_name,
            "type": "sql",
            "query": query,
            "threshold": threshold,
            "severity": severity,
        }

    if rule_type == "library":
        checks = qr.get("checks", {})
        if isinstance(checks, dict):
            for check_type, check_value in checks.items():
                return _convert_library_check(
                    check_type, check_value, rule_name, severity, field_name
                )

    return None


def _convert_library_check(
    check_type: str,
    check_value: Any,
    rule_name: str,
    severity: str,
    field_name: str = "",
) -> dict[str, Any] | None:
    """Convert an ODCS library check to a DataVow rule."""
    check_type_lower = check_type.lower()

    if check_type_lower in ("nullvalues", "missingvalues"):
        if field_name:
            return {
                "name": rule_name or f"not_null_{field_name}",
                "type": "not_null",
                "field": field_name,
                "severity": severity,
            }

    if check_type_lower == "duplicatevalues":
        if field_name:
            return {
                "name": rule_name or f"unique_{field_name}",
                "type": "unique",
                "field": field_name,
                "severity": severity,
            }

    if check_type_lower == "rowcount":
        result: dict[str, Any] = {
            "name": rule_name or "row_count_check",
            "type": "row_count",
            "severity": severity,
        }
        if isinstance(check_value, dict):
            if "min" in check_value:
                result["min"] = check_value["min"]
            if "max" in check_value:
                result["max"] = check_value["max"]
        elif isinstance(check_value, (int, float)):
            result["min"] = int(check_value)
        return result

    return None
