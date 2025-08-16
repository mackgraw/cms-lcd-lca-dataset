from __future__ import annotations
from typing import Dict, Iterable, List, Tuple


def _key(r: Dict[str, str]) -> Tuple[str, str, str]:
    # Identify a unique code row: (article_id, code_system, code)
    return (str(r.get("article_id", "")), str(r.get("code_system", "")), str(r.get("code", "")))


def compute_code_changes(prev_rows: Iterable[Dict[str, str]], curr_rows: Iterable[Dict[str, str]]) -> List[Dict[str, str]]:
    """
    Returns a list of change dicts with fields:
      - change_type: Added | Removed | FlagChanged
      - article_id, code_system, code
      - prev_flag, curr_flag
    """
    prev_map: Dict[Tuple[str, str, str], Dict[str, str]] = {}
    curr_map: Dict[Tuple[str, str, str], Dict[str, str]] = {}

    for r in prev_rows:
        prev_map[_key(r)] = r
    for r in curr_rows:
        curr_map[_key(r)] = r

    changes: List[Dict[str, str]] = []

    # Added / FlagChanged
    for k, curr in curr_map.items():
        if k not in prev_map:
            changes.append(
                {
                    "change_type": "Added",
                    "article_id": curr.get("article_id", ""),
                    "code_system": curr.get("code_system", ""),
                    "code": curr.get("code", ""),
                    "prev_flag": "",
                    "curr_flag": curr.get("coverage_flag", ""),
                }
            )
        else:
            prev = prev_map[k]
            pf, cf = (prev.get("coverage_flag", "") or ""), (curr.get("coverage_flag", "") or "")
            if pf != cf:
                changes.append(
                    {
                        "change_type": "FlagChanged",
                        "article_id": curr.get("article_id", ""),
                        "code_system": curr.get("code_system", ""),
                        "code": curr.get("code", ""),
                        "prev_flag": pf,
                        "curr_flag": cf,
                    }
                )

    # Removed
    for k, prev in prev_map.items():
        if k not in curr_map:
            changes.append(
                {
                    "change_type": "Removed",
                    "article_id": prev.get("article_id", ""),
                    "code_system": prev.get("code_system", ""),
                    "code": prev.get("code", ""),
                    "prev_flag": prev.get("coverage_flag", "") or "",
                    "curr_flag": "",
                }
            )

    return changes
