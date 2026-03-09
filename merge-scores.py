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
from typing import List, Dict, Optional, Set
from copy import deepcopy


# --------------------------
# Utility
# --------------------------
def eprint(*args, **kw):
    print(*args, file=sys.stderr, **kw)


def first_mscx_name(zf: zipfile.ZipFile) -> Optional[str]:
    for name in zf.namelist():
        if name.lower().endswith(".mscx") and not name.endswith("/"):
            return name
    return None


# --------------------------
# MS4 helpers
# --------------------------
def get_score(root: ET.Element) -> ET.Element:
    """Return the <Score> element, accounting for <museScore> wrapper."""
    if root.tag == "Score":
        return root
    s = root.find("Score")
    return s if s is not None else root


def extract_longname(part: ET.Element) -> Optional[str]:
    """Strict longName-only. No fallback to trackName or instrumentId."""
    # Prefer <Instrument><longName>
    for ln in part.findall(".//Instrument/longName"):
        val = (ln.text or "").strip()
        if val:
            return val
    # fallback: <Part><longName>
    ln = part.find("longName")
    if ln is not None and (ln.text or "").strip():
        return (ln.text or "").strip()
    return None


def index_single_staff_parts(root: ET.Element):
    """
    Returns:
      name_to_part: longName -> <Part>
      final_names: order of names as appear
      name_to_staff: longName -> score-level <Staff>
      staff_by_id: id -> <Staff>
    """
    score = get_score(root)
    parts = list(score.findall("Part"))

    name_to_part: Dict[str, ET.Element] = {}
    name_list: List[str] = []

    for p in parts:
        name = extract_longname(p)
        if not name:
            eprint("Warning: Part without longName skipped.")
            continue
        templates = p.findall("Staff")
        if len(templates) > 1:
            eprint(f"Note: Part '{name}' has {len(templates)} staff templates; single-staff mode uses first only.")
        if name in name_to_part:
            eprint(f"Warning: Duplicate longName '{name}' in base; skipping subsequent duplicates.")
            continue
        name_to_part[name] = p
        name_list.append(name)

    # Score-level staffs
    score_staves = [s for s in score.findall("Staff") if "id" in s.attrib]
    n = min(len(name_list), len(score_staves))
    if n != len(name_list) or n != len(score_staves):
        eprint(f"Note: mismatch (parts={len(name_list)} staves={len(score_staves)}); using first {n} pairs.")

    name_to_staff: Dict[str, ET.Element] = {}
    for i in range(n):
        name_to_staff[name_list[i]] = score_staves[i]
    staff_by_id = {s.attrib["id"]: s for s in score_staves}

    # final ordering:
    final_names = [nm for nm in name_list if nm in name_to_staff]
    return name_to_part, final_names, name_to_staff, staff_by_id


# --------------------------
# Measures & placeholders
# --------------------------
def get_measures(staff: ET.Element) -> List[ET.Element]:
    return [ch for ch in staff if ch.tag == "Measure"]


NOTE_TAGS = ["TimeSig", "KeySig", "BarLine", "Clef", "subtype", "sigN", "sigD"]


def measure_timesig_str(ms: ET.Element, fallback_ts: str) -> str:
    """Return the time signature that *applies to this measure* as 'N/D'.
    If the measure carries a <TimeSig>, use it; otherwise inherit fallback_ts.
    """
    voice = ms.find("voice")
    if voice is not None:
        timesig = voice.find("TimeSig")
        if timesig is not None:
            N = timesig.find("sigN")
            D = timesig.find("sigD")
            if N is not None and D is not None and (N.text and D.text):
                return f"{N.text}/{D.text}"
    return fallback_ts


def strip_measure(measure: ET.Element, timesig: str):
    voices = measure.findall("voice")
    measure_len = measure.get("len")
    if measure_len:
        timesig = measure_len
    if voices is None:
        return
    # Strip note-ish tags but keep meta (TimeSig, KeySig, BarLine, Clef, etc.)
    voice = voices[0]
    removed = True
    while removed:
        removed = False
        for parent in list(voice.iter()):
            for ch in list(parent):
                if ch.tag not in NOTE_TAGS:
                    parent.remove(ch)
                    removed = True
    # Insert a full-measure rest that matches the *current* timesig context
    rest = ET.Element("Rest")
    dt = ET.SubElement(rest, "durationType")
    dt.text = "measure"
    dur = ET.SubElement(rest, "duration")
    dur.text = timesig
    voice.append(rest)
    for voice in voices[1:]:
        measure.remove(voice)


def clone_placeholder(m: ET.Element, ts: str) -> ET.Element:
    cp = deepcopy(m)
    strip_measure(cp, ts)
    return cp


# --------------------------
# Section breaks
# --------------------------


def create_section_break() -> ET.Element:
    lb = ET.Element("LayoutBreak")
    st = ET.SubElement(lb, "subtype")
    st.text = "section"
    return lb


def has_section_break(m: ET.Element) -> bool:
    for ch in m:
        if ch.tag != "LayoutBreak":
            continue
        t = ch.get("type")
        if t and t.lower() == "section":
            return True
        for sub in ch:
            if sub.tag == "subtype" and (sub.text or "").strip().lower() == "section":
                return True
    return False


def insert_break(m: ET.Element):
    if has_section_break(m):
        return
    lb = create_section_break()
    kids = list(m)
    eid_i = None
    v_i = None
    for i, ch in enumerate(kids):
        if ch.tag == "eid":
            eid_i = i
        if ch.tag == "voice" and v_i is None:
            v_i = i
    if v_i is not None:
        idx = v_i
        if eid_i is not None and eid_i < v_i:
            idx = eid_i + 1
    else:
        idx = (eid_i + 1) if eid_i is not None else 0
    m.insert(idx, lb)


def last_measure(staff: Optional[ET.Element]) -> Optional[ET.Element]:
    if staff is None:
        return None
    ms = get_measures(staff)
    return ms[-1] if ms else None


# --------------------------
# ID helpers
# --------------------------


def next_id(existing: Set[str]) -> str:
    nums = []
    for x in existing:
        try:
            nums.append(int(x))
        except Exception:
            pass
    base = max(nums) + 1 if nums else 1
    n = base
    while str(n) in existing:
        n += 1
    return str(n)


# --------------------------
# Order copying helpers
# --------------------------


def find_order(score: ET.Element) -> Optional[ET.Element]:
    for ch in score:
        if ch.tag.endswith("Order"):
            return ch
    return None


def copy_instrument_into_order(order: ET.Element, donor_order: ET.Element, instr_id: str, longName: str):
    donor_stub = None
    for it in donor_order.findall("instrument"):
        if it.get("id") == instr_id:
            donor_stub = deepcopy(it)
            break
    if donor_stub is None:
        donor_stub = ET.Element("instrument", {"id": instr_id})
        fam = ET.SubElement(donor_stub, "family")
        fam.set("id", "voices")
        fam.text = "Stimmen"

    children = list(order)
    bottom_tags = {"soloists", "section", "family", "unsorted"}
    idx = len(children)
    for i, ch in enumerate(children):
        if ch.tag in bottom_tags:
            idx = i
            break
    order.insert(idx, donor_stub)


# --------------------------
# VBox handling
# --------------------------


def remove_vboxes(staff: Optional[ET.Element]) -> int:
    if staff is None:
        return 0
    removed = 0
    for ch in list(staff):
        if ch.tag == "VBox":
            staff.remove(ch)
            removed += 1
    return removed


# --------------------------
# New voice creation
# --------------------------


def _build_placeholders_from_reference(ref_staff: ET.Element) -> List[ET.Element]:
    """
    Create per-measure placeholder clones that preserve *actual* measure length.
    Handles pickup measures by respecting <Measure len="X/Y"> if present.
    """
    ref_ms = get_measures(ref_staff)
    placeholders: List[ET.Element] = []
    current_ts = ""  # last known inherited timesig (N/D)

    for m in ref_ms:
        # Priority 1: a pickup measure or irregular bar has explicit len="X/Y"
        if "len" in m.attrib:
            ts = m.attrib["len"]
        else:
            # Priority 2: a regular measure inherits or defines real time signature
            ts = measure_timesig_str(m, current_ts)

        current_ts = ts
        placeholders.append(clone_placeholder(m, ts))

    return placeholders


def create_new_voice(
    base_root: ET.Element,
    donor_root: ET.Element,
    longName: str,
    donor_part: ET.Element,
    donor_staff: ET.Element,
    used_staff_ids: Set[str],
    primary_staff: Optional[ET.Element],
) -> tuple[ET.Element[str], ET.Element[str]]:
    score = get_score(base_root)

    # copy donor <Part>
    new_part = deepcopy(donor_part)
    new_id = next_id({p.get("id", "0") for p in score.findall("Part")})
    new_part.set("id", new_id)

    # keep only first staff template
    st_templates = new_part.findall("Staff")
    for st in st_templates[1:]:
        new_part.remove(st)

    # insert new <Part> before first score-level <Staff>
    idx = len(score)
    for i, el in enumerate(score):
        if el.tag == "Staff":
            idx = i
            break
    score.insert(idx, new_part)

    # create a new score-level <Staff>
    new_staff = deepcopy(donor_staff)
    new_sid = next_id(used_staff_ids)
    used_staff_ids.add(new_sid)
    new_staff.set("id", new_sid)
    score.append(new_staff)

    # backfill placeholders ONLY (no donor measures yet) — **per measure TS**
    if primary_staff is not None:
        placeholders = _build_placeholders_from_reference(primary_staff)
        # wipe existing donor measures on the cloned staff
        for dm in list(new_staff):
            if dm.tag == "Measure":
                new_staff.remove(dm)
        for ph in placeholders:
            new_staff.append(ph)

    # Ensure no VBox remains on the newly created staff; merge will place VBox once
    remove_vboxes(new_staff)

    # copy Order stub if needed
    base_order = find_order(score)
    donor_order = find_order(get_score(donor_root))
    if base_order is not None and donor_order is not None:
        instr = donor_part.find(".//Instrument")
        instr_id = instr.get("id") if instr is not None and instr.get("id") else longName.lower().replace(" ", "_")
        assert isinstance(instr_id, str)
        exists = any(it.get("id") == instr_id for it in base_order.findall("instrument"))
        if not exists:
            copy_instrument_into_order(base_order, donor_order, instr_id, longName)
    else:
        eprint("Warning: Could not locate <Order> in donor/base; instrument stub not copied.")

    return new_part, new_staff


# --------------------------
# XML lenient parser
# --------------------------

_ILLEGAL = re.compile(r"([\x00-\x08\x0B\x0C\x0E-\x1F])")


def sanitize(b: bytes) -> bytes:
    b = b.replace(b"\x00", b"")
    try:
        s = b.decode("utf-8", errors="replace")
    except Exception:
        s = b.decode("latin-1", errors="replace")
    s = _ILLEGAL.sub("", s)
    return s.encode("utf-8")


def truncate_root(b: bytes) -> bytes:
    s = b.decode("utf-8", errors="ignore")
    for closing in ("</museScore>", "</Score>"):
        i = s.rfind(closing)
        if i != -1:
            return s[: i + len(closing)].encode("utf-8")
    return b


def parse_xml_lenient(b: bytes, label: str) -> ET.ElementTree[ET.Element] | None:
    parser = ET.XMLParser()
    try:
        return ET.parse(io.BytesIO(b), parser)
    except ET.ParseError:
        try:
            return ET.parse(io.BytesIO(sanitize(b)), parser)
        except ET.ParseError:
            try:
                return ET.parse(io.BytesIO(truncate_root(sanitize(b))), parser)
            except ET.ParseError:
                eprint(f"Could not parse donor '{label}'.")
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


def main():
    ap = argparse.ArgumentParser(description="Merge MS4 .mscz files")
    ap.add_argument("-o", "--output-name", required=True)
    ap.add_argument("-D", "--output-dir", default=".")
    ap.add_argument("base")
    ap.add_argument("donors", nargs="+")
    args = ap.parse_args()

    base_zip = os.path.abspath(args.base)
    donor_list = [os.path.abspath(x) for x in args.donors]
    out_dir = os.path.abspath(args.output_dir)
    out_path = os.path.join(out_dir, f"{args.output_name}.mscz")

    if not os.path.isfile(base_zip):
        eprint(f"Base file not found: {base_zip}")
        return 66
    for d in donor_list:
        if not os.path.isfile(d):
            eprint(f"Donor file not found: {d}")
            return 66

    work = tempfile.mkdtemp("mscore_merge_")
    base_dir = os.path.join(work, "base")
    ensure_dir(base_dir)

    try:
        # extract base
        with zipfile.ZipFile(base_zip, "r") as z:
            z.extractall(base_dir)
            mscx_name = first_mscx_name(z)
        if not mscx_name:
            eprint("Base has no .mscx")
            return 65

        base_mscx = os.path.join(base_dir, mscx_name)
        raw = open(base_mscx, "rb").read()
        base_tree = ET.parse(io.BytesIO(raw))
        base_root = base_tree.getroot()

        # base indexing
        base_name_to_part, base_names_order, base_name_to_staff, base_staff_by_id = index_single_staff_parts(base_root)
        used_staff_ids = set(base_staff_by_id.keys())
        keys_order = list(base_names_order)
        primary_staff = base_staff_by_id.get("1")

        for donor_path in donor_list:
            label = os.path.basename(donor_path)
            try:
                with zipfile.ZipFile(donor_path, "r") as dz:
                    dn_mscx = first_mscx_name(dz)
                    if not dn_mscx:
                        eprint(f"No .mscx in donor {label}, skipping.")
                        continue
                    donor_bytes = dz.read(dn_mscx)
            except Exception:
                eprint(f"Could not read donor {label}, skipping.")
                continue

            donor_tree = parse_xml_lenient(donor_bytes, label)
            if donor_tree is None:
                continue
            donor_root = donor_tree.getroot()
            if donor_root is None:
                continue

            donor_name_to_part, donor_names_order, donor_name_to_staff, donor_staff_by_id = index_single_staff_parts(
                donor_root
            )

            # Insert section break on last base measure before adding this donor
            if primary_staff is not None:
                lm = last_measure(primary_staff)
                if lm is not None:
                    insert_break(lm)

            # ---- Place donor VBox BEFORE any donor measures are appended
            donor_first_name = donor_names_order[0] if donor_names_order else None
            donor_first_staff = donor_name_to_staff.get(donor_first_name) if donor_first_name else None
            if primary_staff is not None and donor_first_staff is not None:
                # Find first VBox on donor first staff
                for ch in donor_first_staff:
                    if ch.tag == "VBox":
                        primary_staff.append(deepcopy(ch))
                        break
            # ---- end VBox placement

            # New voices (create empty staves with placeholders)
            base_names = set(keys_order)
            new_voices = [nm for nm in donor_names_order if nm not in base_names]
            for nm in new_voices:
                dp = donor_name_to_part[nm]
                ds = donor_name_to_staff[nm]
                new_part, new_staff = create_new_voice(base_root, donor_root, nm, dp, ds, used_staff_ids, primary_staff)
                base_name_to_staff[nm] = new_staff
                base_name_to_part[nm] = new_part
                keys_order.append(nm)

            # Build placeholder template from donor *per-measure* TS
            donor_ref_staff = donor_first_staff
            donor_placeholders: List[ET.Element] = []
            if donor_ref_staff is not None:
                donor_placeholders = _build_placeholders_from_reference(donor_ref_staff)

            # Merge donor contents per voice
            for nm in keys_order:
                bs = base_name_to_staff.get(nm)
                if bs is None:
                    continue
                if nm in donor_name_to_staff:
                    ds = donor_name_to_staff[nm]
                    for ch in list(ds):
                        if ch.tag == "VBox":
                            # skip here; VBox already placed once on primary_staff
                            continue
                        bs.append(deepcopy(ch))
                else:
                    for ph in donor_placeholders:
                        bs.append(deepcopy(ph))

        # write final
        buf = io.BytesIO()
        base_tree.write(buf, encoding="utf-8", xml_declaration=True)
        open(base_mscx, "wb").write(buf.getvalue())
        shutil.move(base_mscx, os.path.join(base_dir, f"{args.output_name}.mscx"))
        ensure_dir(out_dir)
        write_zip_from_dir(base_dir, out_path)
        print(f"OK: merged archive created at: {out_path}")
    finally:
        shutil.rmtree(work, ignore_errors=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
