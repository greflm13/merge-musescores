#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Merge donor .mscz into a base .mscz by appending <Staff id="n"> contents
"""

from __future__ import annotations

import argparse
import io
import os
import re
import shutil
import sys
import tempfile
import zipfile
import xml.etree.ElementTree as ET
from copy import deepcopy
from typing import Optional, List, Dict


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def first_mscx_name(zf: zipfile.ZipFile) -> Optional[str]:
    for name in zf.namelist():
        if name.lower().endswith(".mscx") and not name.endswith("/"):
            return name
    return None


def read_doctype(text: str) -> Optional[str]:
    m = re.search(r"<!DOCTYPE[^>]*>", text, flags=re.IGNORECASE | re.DOTALL)
    return m.group(0) if m else None


def inject_doctype(xml_bytes: bytes, doctype: Optional[str]) -> bytes:
    if not doctype:
        return xml_bytes
    text = xml_bytes.decode("utf-8")
    if text.startswith("<?xml"):
        pos = text.find("?>")
        if pos != -1:
            return (text[: pos + 2] + "\n" + doctype + "\n" + text[pos + 2 :]).encode("utf-8")
    return (doctype + "\n" + text).encode("utf-8")


def find_staffs(root: ET.Element) -> List[ET.Element]:
    return [s for s in root.findall(".//Staff") if "id" in s.attrib]


def pick_primary_staff_id(staff_by_id: Dict[str, ET.Element]) -> Optional[str]:
    if "1" in staff_by_id:
        return "1"
    numeric, other = [], []
    for sid in staff_by_id:
        try:
            numeric.append((int(sid), sid))
        except ValueError:
            other.append(sid)
    if numeric:
        numeric.sort()
        return numeric[0][1]
    return min(other) if other else None


# --------------------------
# Section break utilities
# --------------------------


def create_section_break() -> ET.Element:
    lb = ET.Element("LayoutBreak")
    st = ET.SubElement(lb, "subtype")
    st.text = "section"
    return lb


def measure_has_section_break(measure: ET.Element) -> bool:
    """
    Detect a section break inside <Measure>.
    Accept both:
      - <LayoutBreak><subtype>section|Section</subtype>...</LayoutBreak>
      - <LayoutBreak type="Section"|"section" .../>
    """
    for ch in list(measure):
        if ch.tag != "LayoutBreak":
            continue
        t = ch.get("type")
        if t and t.lower() == "section":
            return True
        # Check nested <subtype>
        for sub in list(ch):
            if sub.tag == "subtype" and (sub.text or "").strip().lower() == "section":
                return True
    return False


def insert_break_into_measure(measure: ET.Element) -> None:
    """
    Insert LayoutBreak inside <Measure>, before the first <voice>, and right after <eid> if present.
    If a section break is already present, do nothing.
    """
    if measure_has_section_break(measure):
        return

    lb = create_section_break()
    children = list(measure)

    eid_index = None
    first_voice_index = None
    for i, ch in enumerate(children):
        if ch.tag == "eid" and eid_index is None:
            eid_index = i
        if ch.tag == "voice" and first_voice_index is None:
            first_voice_index = i

    if first_voice_index is not None:
        ins_index = first_voice_index
        if eid_index is not None and eid_index < first_voice_index:
            ins_index = eid_index + 1
        measure.insert(ins_index, lb)
    else:
        ins_index = (eid_index + 1) if eid_index is not None else 0
        measure.insert(ins_index, lb)


def last_measure_of_staff(staff_elem: Optional[ET.Element]) -> Optional[ET.Element]:
    if staff_elem is None:
        return None
    measures = [ch for ch in list(staff_elem) if ch.tag == "Measure"]
    return measures[-1] if measures else None


# --------------------------
# Appending logic
# --------------------------


def append_staff_children_and_get_last_appended_measure_primary(
    base_root: ET.Element,
    donor_root: ET.Element,
    primary_sid: Optional[str],
) -> Optional[ET.Element]:
    """
    Append donor <Staff id="..."> children into base.
    Return the last appended <Measure> for the PRIMARY staff (primary_sid), or None.
    """
    base_staff_by_id: Dict[str, ET.Element] = {s.attrib["id"]: s for s in find_staffs(base_root)}
    donor_staffs = find_staffs(donor_root)

    if not donor_staffs:
        eprint("Warning: Donor has no <Staff id='...'> elements; skipping.")
        return None

    last_appended_measure_primary: Optional[ET.Element] = None

    for ds in donor_staffs:
        sid = ds.attrib.get("id")
        bs = base_staff_by_id.get(sid)
        if bs is None:
            eprint(f"Warning: Base has no <Staff id='{sid}'>; skipping donor staff.")
            continue

        for child in list(ds):
            copy_child = deepcopy(child)
            bs.append(copy_child)
            if sid == primary_sid and copy_child.tag == "Measure":
                last_appended_measure_primary = copy_child

    return last_appended_measure_primary


# --------------------------
# Lenient donor XML parsing
# --------------------------

_ILLEGAL_XML_CHARS_RE = re.compile(
    r"([\x00-\x08\x0B\x0C\x0E-\x1F])"  # control chars not allowed by XML 1.0 (except \t,\n,\r)
)


def _sanitize_xml_bytes(b: bytes) -> bytes:
    b = b.replace(b"\x00", b"")
    try:
        s = b.decode("utf-8", errors="replace")
    except Exception:
        s = b.decode("latin-1", errors="replace")
    s = _ILLEGAL_XML_CHARS_RE.sub("", s)
    return s.encode("utf-8")


def _truncate_to_last_root_end(b: bytes) -> bytes:
    s = b.decode("utf-8", errors="ignore")
    for closing in ("</museScore>", "</Score>"):
        idx = s.rfind(closing)
        if idx != -1:
            end = idx + len(closing)
            return s[:end].encode("utf-8")
    return b


def parse_xml_lenient(xml_bytes: bytes, label: str) -> Optional[ET.ElementTree]:
    parser = ET.XMLParser()
    try:
        return ET.parse(io.BytesIO(xml_bytes), parser=parser)
    except ET.ParseError as e:
        eprint(f"Warning: Strict parse failed for {label}: {e}. Trying lenient recovery...")

    cleaned = _sanitize_xml_bytes(xml_bytes)
    try:
        return ET.parse(io.BytesIO(cleaned), parser=parser)
    except ET.ParseError as e:
        eprint(f"Warning: Parse after sanitizing failed for {label}: {e}. Trying truncation...")

    truncated = _truncate_to_last_root_end(cleaned)
    try:
        return ET.parse(io.BytesIO(truncated), parser=parser)
    except ET.ParseError as e:
        eprint(f"Warning: Truncated parse failed for {label}: {e}. Giving up.")
        return None


# --------------------------
# Zip helpers
# --------------------------


def ensure_dir(path: str) -> None:
    if path:
        os.makedirs(path, exist_ok=True)


def write_zip_from_dir(src_dir: str, out_zip: str) -> None:
    ensure_dir(os.path.dirname(os.path.abspath(out_zip)) or ".")
    with zipfile.ZipFile(out_zip, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for root, _dirs, files in os.walk(src_dir):
            for fname in files:
                abs_path = os.path.join(root, fname)
                rel_path = os.path.relpath(abs_path, src_dir).replace(os.sep, "/")
                z.write(abs_path, rel_path)


# --------------------------
# Main
# --------------------------


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Merge donor .mscz into base .mscz (append Staff contents) and add section breaks at boundaries."
    )
    ap.add_argument("-o", "--output-name", required=True, help="Output filename (without extension).")
    ap.add_argument("-D", "--output-dir", default=".", help="Output directory. Default: .")
    ap.add_argument("base", help="Base .mscz file")
    ap.add_argument("donors", nargs="+", help="Donor .mscz files (order matters)")
    args = ap.parse_args()

    base_zip_path = os.path.abspath(args.base)
    donor_zip_paths = [os.path.abspath(p) for p in args.donors]
    out_dir = os.path.abspath(args.output_dir)
    out_path = os.path.join(out_dir, f"{args.output_name}.mscz")

    if not os.path.isfile(base_zip_path):
        eprint(f"Error: Base file not found: {base_zip_path}")
        return 66
    for p in donor_zip_paths:
        if not os.path.isfile(p):
            eprint(f"Error: Donor file not found: {p}")
            return 66

    workdir = tempfile.mkdtemp(prefix="mscz_merge_")
    base_dir = os.path.join(workdir, "base")
    ensure_dir(base_dir)

    try:
        with zipfile.ZipFile(base_zip_path, "r") as z:
            z.extractall(base_dir)
            base_mscx_name = first_mscx_name(z)
        if not base_mscx_name:
            eprint(f"Error: No .mscx file found inside base archive: {base_zip_path}")
            return 65

        base_mscx_path = os.path.join(base_dir, base_mscx_name.replace("/", os.sep))
        if not os.path.isfile(base_mscx_path):
            found = []
            for root, _dirs, files in os.walk(base_dir):
                for f in files:
                    if f.lower().endswith(".mscx"):
                        found.append(os.path.join(root, f))
            if not found:
                eprint(f"Error: Extracted base does not contain any .mscx: {base_zip_path}")
                return 65
            base_mscx_path = sorted(found)[0]

        with open(base_mscx_path, "rb") as f:
            base_raw = f.read()
        base_doctype = read_doctype(base_raw.decode("utf-8", errors="ignore"))

        base_tree = ET.parse(io.BytesIO(base_raw), parser=ET.XMLParser())
        base_root = base_tree.getroot()

        base_staff_by_id: Dict[str, ET.Element] = {s.attrib["id"]: s for s in find_staffs(base_root)}
        primary_sid = pick_primary_staff_id(base_staff_by_id)
        primary_staff_elem = base_staff_by_id.get(primary_sid) if primary_sid else None

        for donor_path in donor_zip_paths:
            donor_label = os.path.basename(donor_path)
            try:
                with zipfile.ZipFile(donor_path, "r") as dz:
                    donor_mscx_name = first_mscx_name(dz)
                    if not donor_mscx_name:
                        eprint(f"Warning: No .mscx inside donor '{donor_label}'; skipping.")
                        continue
                    donor_bytes = dz.read(donor_mscx_name)
            except zipfile.BadZipFile as e:
                eprint(f"Warning: Donor '{donor_label}' is not a valid zip: {e}; skipping.")
                continue
            except KeyError as e:
                eprint(f"Warning: Donor '{donor_label}' missing expected file: {e}; skipping.")
                continue

            donor_tree = parse_xml_lenient(donor_bytes, donor_label)
            if donor_tree is None:
                eprint(f"Warning: Could not parse donor '{donor_label}' even after recovery; skipping.")
                continue

            pre_last_measure = last_measure_of_staff(primary_staff_elem)
            if pre_last_measure is not None:
                insert_break_into_measure(pre_last_measure)
            else:
                eprint(
                    f"Warning: No measure found in primary staff to place pre-append break before donor '{donor_label}'."
                )

            last_appended = append_staff_children_and_get_last_appended_measure_primary(
                base_root, donor_tree.getroot(), primary_sid
            )

            if last_appended is None and primary_staff_elem is not None:
                last_appended = last_measure_of_staff(primary_staff_elem)

            if last_appended is not None:
                insert_break_into_measure(last_appended)
            else:
                eprint(f"Warning: No measure found to attach post-append break for donor '{donor_label}'.")

        buf = io.BytesIO()
        base_tree.write(buf, encoding="utf-8", xml_declaration=True)
        final_bytes = inject_doctype(buf.getvalue(), base_doctype)

        with open(base_mscx_path, "wb") as f:
            f.write(final_bytes)

        ensure_dir(out_dir)
        write_zip_from_dir(base_dir, out_path)
        print(f"OK: merged archive created at: {out_path}")

    finally:
        shutil.rmtree(workdir, ignore_errors=True)

    return 0


if __name__ == "__main__":
    sys.exit(main())
