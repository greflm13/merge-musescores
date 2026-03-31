#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Merge donor .mscz into a base .mscz by appending <Staff id="n"> contents
"""

from __future__ import annotations

import io
import os
import re
import sys
import json
import gzip
import shutil
import zipfile
import logging
import tempfile
import argparse
import datetime
import xml.etree.ElementTree as ET

from copy import deepcopy
from typing import List, Dict, Optional, Set


SCRIPTDIR = os.path.dirname(os.path.realpath(__file__)).removesuffix(__package__ if __package__ else "")


class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_entry = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Add any extra fields passed via extra parameter
        standard_fields = {
            "name",
            "msg",
            "args",
            "levelname",
            "levelno",
            "pathname",
            "filename",
            "module",
            "exc_info",
            "exc_text",
            "stack_info",
            "lineno",
            "funcName",
            "created",
            "msecs",
            "relativeCreated",
            "thread",
            "threadName",
            "processName",
            "process",
            "message",
            "asctime",
        }

        for key, value in record.__dict__.items():
            if key not in standard_fields:
                log_entry[key] = value

        return json.dumps(log_entry)


LOG_DIR = os.path.join(SCRIPTDIR, "logs")
LATEST_LOG_FILE = os.path.join(LOG_DIR, "latest.jsonl")
os.makedirs(LOG_DIR, exist_ok=True)

logger = logging.getLogger()

file_handler = logging.FileHandler(LATEST_LOG_FILE, encoding="utf-8")
file_handler.setFormatter(JSONFormatter())
file_handler.setLevel(logging.DEBUG)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
console_handler.setLevel(logging.INFO)
logger.addHandler(console_handler)

logger.setLevel(logging.DEBUG)


# --------------------------
# Utility
# --------------------------
def rotate_log_file(compress=True) -> None:
    """
    Truncates the 'latest.jsonl' file after optionally compressing its contents to a timestamped file.
    The 'latest.jsonl' file is not deleted or moved, just emptied.

    Args:
        compress (bool): If True, compress the old log file using gzip.
    """
    if os.path.exists(LATEST_LOG_FILE):
        with open(LATEST_LOG_FILE, "r+", encoding="utf-8") as f:
            first_line = f.readline()
            try:
                first_log = json.loads(first_line)
                first_timestamp = first_log.get("timestamp")
                first_timestamp = first_timestamp.split(",")[0]
            except (json.JSONDecodeError, KeyError):
                first_timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

            safe_timestamp = first_timestamp.replace(":", "-").replace(" ", "_")
            old_log_filename = os.path.join(LOG_DIR, f"{safe_timestamp}.jsonl")

            # Write contents to the new file
            with open(old_log_filename, "w", encoding="utf-8") as old_log_file:
                f.seek(0)  # Go back to the beginning of the file
                shutil.copyfileobj(f, old_log_file)

            if compress:
                with open(old_log_filename, "rb") as f_in:
                    with gzip.open(f"{old_log_filename}.gz", "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)
                os.remove(old_log_filename)

            f.seek(0)
            f.truncate()


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
    for ln in part.findall(".//Instrument/longName"):
        val = (ln.text or "").strip()
        if val:
            return val
    ln = part.find("longName")
    if ln is not None and (ln.text or "").strip():
        return (ln.text or "").strip()
    return None


def index_single_staff_parts(
    root: ET.Element, file: str
) -> tuple[Dict[str, ET.Element[str]], list[str], Dict[str, List[ET.Element[str]]], dict[str, ET.Element[str]]]:
    logger.debug("Indexing parts for file", extra={"file": file})
    score = get_score(root)
    parts = list(score.findall("Part"))
    logger.debug("Found parts", extra={"parts_count": len(parts)})

    name_to_part: Dict[str, ET.Element] = {}
    name_list: List[str] = []

    for p in parts:
        name = extract_longname(p)

        if not name:
            logger.warning("Part without longName skipped", extra={"file": file})
            continue

        if name in name_to_part:
            logger.warning("Duplicate longName skipped", extra={"name": name, "file": file})
            continue

        name_to_part[name] = p
        name_list.append(name)

    score_staves = [s for s in score.findall("Staff") if "id" in s.attrib]
    logger.debug("Found score staves", extra={"staves_count": len(score_staves)})

    name_to_staves: Dict[str, List[ET.Element]] = {}

    cursor = 0

    for name in name_list:
        part = name_to_part[name]

        templates = part.findall("Staff")
        count = len(templates)
        logger.debug("Part staff templates", extra={"part_name": name, "template_count": count})

        if cursor + count > len(score_staves):
            logger.warning("Staff mismatch", extra={"file": file})
            break

        name_to_staves[name] = score_staves[cursor : cursor + count]

        cursor += count

    staff_by_id = {s.attrib["id"]: s for s in score_staves}

    final_names = [nm for nm in name_list if nm in name_to_staves]
    logger.debug("Final indexed parts", extra={"final_names": final_names})

    return name_to_part, final_names, name_to_staves, staff_by_id


# --------------------------
# Measures & placeholders
# --------------------------


def get_measures(staff: ET.Element) -> List[ET.Element]:
    return [ch for ch in staff if ch.tag == "Measure"]


NOTE_TAGS = ["TimeSig", "KeySig", "BarLine", "Clef", "subtype", "sigN", "sigD", "concertKey"]


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


def strip_measure(measure: ET.Element, timesig: str) -> None:
    logger.debug("Stripping measure to placeholder", extra={"timesig": timesig})
    voices = measure.findall("voice")
    measure_len = measure.get("len")
    if measure_len:
        timesig = measure_len
    if voices is None:
        return
    voice = voices[0]
    for parent in list(voice.iter()):
        for ch in list(parent):
            if ch.tag not in NOTE_TAGS:
                parent.remove(ch)

    rest = ET.Element("Rest")
    dt = ET.SubElement(rest, "durationType")
    dt.text = "measure"
    dur = ET.SubElement(rest, "duration")
    dur.text = timesig
    voice.append(rest)

    for voice in voices[1:]:
        measure.remove(voice)


def clone_placeholder(m: ET.Element, ts: str) -> ET.Element:
    logger.debug("Cloning placeholder measure", extra={"original_timesig": ts})
    cp = deepcopy(m)
    strip_measure(cp, ts)
    logger.debug("Placeholder measure cloned and stripped")
    return cp


# --------------------------
# Section breaks
# --------------------------


def create_section_break() -> ET.Element:
    lb = ET.Element("LayoutBreak")
    st = ET.SubElement(lb, "subtype")
    st.text = "section"
    return lb


def create_page_break() -> ET.Element:
    lb = ET.Element("LayoutBreak")
    st = ET.SubElement(lb, "subtype")
    st.text = "page"
    return lb


def has_section_break(m: ET.Element) -> bool:
    for ch in m:
        if ch.tag != "LayoutBreak":
            continue
        for sub in ch:
            if sub.tag == "subtype" and (sub.text or "").strip().lower() == "section":
                return True
    return False


def has_page_break(m: ET.Element) -> bool:
    for ch in m:
        if ch.tag != "LayoutBreak":
            continue
        for sub in ch:
            if sub.tag == "subtype" and (sub.text or "").strip().lower() == "page":
                return True
    return False


def ensure_measure_end_barline(m: ET.Element) -> None:
    """
    Ensure the first voice of the measure contains:
        <BarLine><subtype>end</subtype></BarLine>
    """

    voice = m.find("voice")
    if voice is None:
        return

    # already present?
    for bl in voice.findall("BarLine"):
        st = bl.find("subtype")
        if st is not None and (st.text or "").strip() == "end":
            return

    logger.debug("Adding end barline to measure")
    bl = ET.Element("BarLine")
    st = ET.SubElement(bl, "subtype")
    st.text = "end"

    voice.append(bl)


def ensure_end_barline(m: ET.Element) -> None:
    """
    Ensure the first <voice> in the measure ends with:
        <BarLine><subtype>end</subtype></BarLine>
    """
    voice = m.find("voice")
    if voice is None:
        return

    for bl in voice.findall("BarLine"):
        st = bl.find("subtype")
        if st is not None and (st.text or "").strip() == "end":
            return

    logger.debug("Ensuring end barline in measure")
    bl = ET.Element("BarLine")
    st = ET.SubElement(bl, "subtype")
    st.text = "end"
    voice.append(bl)


def insert_break(m: ET.Element, p: bool) -> None:
    logger.debug("Inserting break into measure", extra={"page_break": p})
    if has_section_break(m):
        if p:
            if has_page_break(m):
                ensure_end_barline(m)
                return
        ensure_end_barline(m)
        return

    lb = create_section_break()
    pb = create_page_break()

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

    if p:
        logger.debug("Inserting page break")
        m.insert(idx, pb)
    logger.debug("Inserting section break")
    m.insert(idx, lb)

    ensure_end_barline(m)


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


def hide_empty_voices(score: ET.Element) -> None:
    he = ET.Element("hideWhenEmpty")
    he.text = "on"
    parts = score.findall("Part")
    added = 0
    for part in list(parts):
        if part.find("hideWhenEmpty") is None:
            part.append(he)
            added += 1
    logger.debug("Added hideWhenEmpty to parts", extra={"added_count": added})


def find_order(score: ET.Element) -> Optional[ET.Element]:
    for ch in score:
        if ch.tag.endswith("Order"):
            return ch
    return None


def copy_instrument_into_order(order: ET.Element, donor_order: ET.Element, instr_id: str) -> None:
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


def reorder_parts_inplace(score: ET.Element) -> None:
    """
    Reorder <Part> elements IN PLACE so all soloists are first.
    Does NOT touch <Staff>. Uses remove+insert for correct moves.
    """

    parts = [el for el in score if el.tag == "Part"]
    soloists_count = len([p for p in parts if _is_solo_part(p)])
    logger.debug("Reordering parts", extra={"parts_count": len(parts), "soloists_count": soloists_count})

    solo = [p for p in parts if _is_solo_part(p)]
    non = [p for p in parts if not _is_solo_part(p)]
    desired = solo + non

    children = list(score)
    part_positions = [i for i, ch in enumerate(children) if ch.tag == "Part"]

    pos_iter = iter(part_positions)
    moves = 0

    for target_part in desired:
        try:
            target_index = next(pos_iter)
        except StopIteration:
            break

        if score[target_index] is target_part:
            continue

        score.remove(target_part)
        score.insert(target_index, target_part)

        children = list(score)
        moves += 1

    logger.debug("Performed part reordering moves", extra={"moves_count": moves})


def _is_solo_part(p: ET.Element) -> bool:
    sol = p.find("soloist")
    return sol is not None and (sol.text or "").strip() == "1"


def _collect_parts(score: ET.Element) -> list[ET.Element]:
    """Score-level <Part> elements, in document order"""
    return [el for el in score if el.tag == "Part"]


def _collect_score_staves(score: ET.Element) -> list[ET.Element]:
    """Score-level <Staff> elements (not the templates under <Part>), in document order"""
    return [el for el in score if el.tag == "Staff" and "id" in el.attrib]


def compute_soloist_permutation_from_current_parts(score: ET.Element) -> list[int]:
    """
    Compute a permutation of indices for the *current* Parts so that soloists come first.
    We only read current XML state; no reliance on name_to_* maps.
    """
    parts = _collect_parts(score)
    logger.debug("Computing soloist permutation", extra={"total_parts": len(parts)})
    solo_idx = [i for i, p in enumerate(parts) if _is_solo_part(p)]
    non_idx = [i for i in range(len(parts)) if i not in solo_idx]
    permutation = solo_idx + non_idx
    logger.debug("Soloist permutation computed", extra={"solo_indices": solo_idx, "non_solo_indices": non_idx, "final_permutation": permutation})
    return permutation


def _reorder_block_inplace_by_permutation(
    score: ET.Element, tag: str, base_elems: list[ET.Element], perm: list[int]
) -> None:
    """
    In-place reorder of a homogeneous block (<Staff> here) to follow a permutation that
    was computed on the *Parts*. 'base_elems' must be a snapshot of the block BEFORE any moves.
    We use remove+insert for a single node at a time to avoid duplicates.
    """
    logger.debug("Reordering block by permutation", extra={"tag": tag, "element_count": len(base_elems), "permutation": perm})
    children = list(score)
    tag_positions = [i for i, ch in enumerate(children) if ch.tag == tag]

    desired = [base_elems[i] for i in perm if i < len(base_elems)]
    logger.debug("Desired element order computed", extra={"desired_count": len(desired)})

    pos_iter = iter(tag_positions)
    moves = 0
    for node in desired:
        try:
            target_pos = next(pos_iter)
        except StopIteration:
            break
        if score[target_pos] is node:
            continue
        score.remove(node)
        score.insert(target_pos, node)
        moves += 1

    logger.debug("Block reordering completed", extra={"moves_made": moves})


def reorder_staves_to_match_parts_soloists_first(score: ET.Element) -> None:
    """
    Compute soloist-first permutation from current Parts and mirror that onto
    score-level Staves (single-staff mode). Does NOT touch Part order or IDs.
    """
    parts_snapshot = _collect_parts(score)
    staves_snapshot = _collect_score_staves(score)
    logger.debug("Reordering staves to match parts", extra={"staves_count": len(staves_snapshot), "parts_count": len(parts_snapshot)})
    if not parts_snapshot or not staves_snapshot:
        logger.debug("No parts or staves to reorder")
        return

    perm = compute_soloist_permutation_from_current_parts(score)
    logger.debug("Computed permutation", extra={"permutation": perm})
    _reorder_block_inplace_by_permutation(score, "Staff", staves_snapshot, perm)


def renumber_staff_ids_sequential(score: ET.Element) -> None:
    """
    After reordering staves, set only <Staff id> to 1..N (in their current order).
    Does NOT touch <Part id>.
    """
    staves = _collect_score_staves(score)
    logger.debug("Renumbering staff IDs sequentially", extra={"staves_count": len(staves)})
    for i, st in enumerate(staves, start=1):
        old_id = st.get("id")
        st.set("id", str(i))
        logger.debug("Staff ID change", extra={"old_id": old_id, "new_id": i})


# --------------------------
# VBox handling
# --------------------------


def remove_hvboxes(staff: Optional[ET.Element]) -> int:
    if staff is None:
        return 0
    removed = 0
    for ch in list(staff):
        if ch.tag == "VBox" or ch.tag == "HBox":
            staff.remove(ch)
            removed += 1
    return removed


def measure_index_of_node(staff: ET.Element, node: ET.Element) -> int:
    count = 0
    for ch in staff:
        if ch is node:
            break
        if ch.tag == "Measure":
            count += 1
    return count


def insert_before_measure_ordinal(first_staff: ET.Element, m: int, vb: ET.Element) -> None:
    if m <= 0:
        for i, ch in enumerate(list(first_staff)):
            if ch.tag == "Measure":
                first_staff.insert(i, vb)
                return
        first_staff.append(vb)
        return
    seen = 0
    for i, ch in enumerate(list(first_staff)):
        if ch.tag == "Measure":
            if seen == m:
                first_staff.insert(i, vb)
                return
            seen += 1
    first_staff.append(vb)


def relocate_hvboxes_to_first_staff_by_measure_ordinal(score: ET.Element) -> None:
    """
    Collect all H/VBoxes from all score-level staves and reinsert them into the
    final first staff (current score order), preserving *measure ordinal*:
      - If a H/VBox had k measures before it on its original staff, insert it
        after the k-th measure of the first staff (k=0 => before first measure).
      - If k >= number of measures in the first staff, append at the end.
    """
    staves = [el for el in score if el.tag == "Staff" and "id" in el.attrib]
    if not staves:
        logger.debug("No staves found for HVBox relocation")
        return
    first_staff = staves[0]

    vboxes = []
    hboxes = []
    for staff in staves:
        for ch in list(staff):
            if ch.tag == "VBox":
                vboxes.append((measure_index_of_node(staff, ch), ch, staff))
            elif ch.tag == "HBox":
                hboxes.append((measure_index_of_node(staff, ch), ch, staff))

    logger.debug("Found boxes to relocate", extra={"vboxes_count": len(vboxes), "hboxes_count": len(hboxes)})

    if not vboxes and not hboxes:
        return

    for _, vb, src_staff in vboxes:
        try:
            src_staff.remove(vb)
        except Exception:
            pass

    for _, hb, src_staff in hboxes:
        try:
            src_staff.remove(hb)
        except Exception:
            pass

    vboxes_sorted = sorted(enumerate(vboxes), key=lambda t: (t[1][0], t[0]))
    hboxes_sorted = sorted(enumerate(hboxes), key=lambda t: (t[1][0], t[0]))
    for _, (m_idx, vb, _) in vboxes_sorted:
        logger.debug("Inserting VBox at measure ordinal", extra={"measure_ordinal": m_idx})
        insert_before_measure_ordinal(first_staff, m_idx, vb)
    for _, (m_idx, hb, _) in hboxes_sorted:
        logger.debug("Inserting HBox at measure ordinal", extra={"measure_ordinal": m_idx})
        insert_before_measure_ordinal(first_staff, m_idx, hb)


# --------------------------
# New voice creation
# --------------------------


def _build_placeholders_from_reference(ref_staff: ET.Element) -> List[ET.Element]:
    """
    Create per-measure placeholder clones that preserve *actual* measure length.
    Handles pickup measures by respecting <Measure len="X/Y"> if present.

    LayoutBreaks and end BarLines are copied from the reference measure so
    section boundaries remain consistent across all staffs.
    """

    ref_ms = get_measures(ref_staff)
    logger.debug("Building placeholders from reference measures", extra={"reference_measures_count": len(ref_ms)})

    placeholders: List[ET.Element] = []
    current_ts = ""

    for i, m in enumerate(ref_ms):
        if "len" in m.attrib:
            ts = m.attrib["len"]
        else:
            ts = measure_timesig_str(m, current_ts)

        current_ts = ts
        logger.debug("Measure timesig", extra={"measure_number": i + 1, "timesig": ts})

        cp = clone_placeholder(m, ts)

        insert_index = 0
        layout_breaks_copied = 0
        for ch in m:
            if ch.tag == "LayoutBreak":
                cp.insert(insert_index, deepcopy(ch))
                insert_index += 1
                layout_breaks_copied += 1

        src_voice = m.find("voice")
        dst_voice = cp.find("voice")

        barlines_copied = 0
        if src_voice is not None and dst_voice is not None:
            for bl in src_voice.findall("BarLine"):
                st = bl.find("subtype")
                if st is not None and (st.text or "").strip() == "end":
                    dst_voice.append(deepcopy(bl))
                    barlines_copied += 1

        logger.debug("Placeholder built for measure", extra={"measure_number": i + 1, "layout_breaks_copied": layout_breaks_copied, "barlines_copied": barlines_copied})

        placeholders.append(cp)

    logger.debug("Built placeholders", extra={"placeholders_count": len(placeholders)})
    return placeholders


def relocate_system_spanners_to_first_staff(score: ET.Element) -> None:
    """
    Move system spanners (Volta, GradualTempoChange, etc.)
    to the first staff so they remain visible when solo staves hide.
    """

    staves = [s for s in score if s.tag == "Staff" and "id" in s.attrib]
    if not staves:
        logger.debug("No staves found for spanner relocation")
        return

    first_staff = staves[0]
    first_measures = get_measures(first_staff)
    logger.debug("Relocating system spanners to first staff", extra={"source_staves_count": len(staves) - 1})

    SYSTEM_SPANNERS = {"Volta", "GradualTempoChange"}
    relocated_count = 0

    logger.debug("Scanning staves for system spanners", extra={"staves_to_scan": len(staves) - 1})
    for staff_idx, staff in enumerate(staves[1:], start=2):  # start=2 because we skip first staff
        measures = get_measures(staff)
        logger.debug("Scanning staff for spanners", extra={"staff_id": staff.get("id"), "measures_count": len(measures)})

        for i, m in enumerate(measures):
            if i >= len(first_measures):
                logger.debug("Skipping measure beyond first staff length", extra={"measure_index": i, "first_staff_measures": len(first_measures)})
                continue

            voice = m.find("voice")
            if voice is None:
                continue

            dst_voice = first_measures[i].find("voice")
            if dst_voice is None:
                continue

            for el in list(voice):
                if el.tag != "Spanner":
                    continue

                if el.get("type") not in SYSTEM_SPANNERS:
                    continue

                sp_type = el.get("type")

                if sp_type is None:
                    continue

                if el.find(sp_type) is None and el.find("prev") is None:
                    continue

                logger.debug("Relocating spanner from staff to first staff", extra={"spanner_type": sp_type, "staff_id": staff.get("id"), "measure_index": i})
                voice.remove(el)
                dst_voice.insert(0, el)
                relocated_count += 1

    logger.debug("Relocated system spanners", extra={"relocated_count": relocated_count})


def relocate_system_texts_to_first_staff(score: ET.Element) -> None:
    """
    Move system text (Tempo etc.)
    to the first staff so they remain visible when solo staves hide.
    """

    staves = [s for s in score if s.tag == "Staff" and "id" in s.attrib]
    if not staves:
        logger.debug("No staves found for text relocation")
        return

    first_staff = staves[0]
    first_measures = get_measures(first_staff)
    logger.debug("Relocating system texts to first staff", extra={"source_staves_count": len(staves) - 1})

    SYSTEM_TEXTS = {"Tempo"}
    relocated_count = 0

    logger.debug("Scanning staves for system texts", extra={"staves_to_scan": len(staves) - 1})
    for staff_idx, staff in enumerate(staves[1:], start=2):  # start=2 because we skip first staff
        measures = get_measures(staff)
        logger.debug("Scanning staff for texts", extra={"staff_id": staff.get("id"), "measures_count": len(measures)})

        for i, m in enumerate(measures):
            if i >= len(first_measures):
                continue

            voice = m.find("voice")
            if voice is None:
                continue

            dst_voice = first_measures[i].find("voice")
            if dst_voice is None:
                continue

            for el in list(voice):
                if el.tag not in SYSTEM_TEXTS:
                    continue

                logger.debug("Relocating text from staff to first staff", extra={"element_tag": el.tag, "staff_id": staff.get("id"), "measure_index": i})
                voice.remove(el)
                dst_voice.insert(0, el)
                relocated_count += 1

    logger.debug("Relocated system texts", extra={"relocated_count": relocated_count})


def create_new_voice(
    base_root: ET.Element,
    donor_root: ET.Element,
    longName: str,
    donor_part: ET.Element,
    donor_staff: ET.Element,
    used_staff_ids: Set[str],
    primary_staff: Optional[ET.Element],
) -> tuple[ET.Element[str], ET.Element[str]]:
    logger.debug("Creating new voice", extra={"longName": longName})
    score = get_score(base_root)
    new_part = deepcopy(donor_part)
    new_id = next_id(used_staff_ids)
    logger.debug("New part ID", extra={"new_id": new_id})
    new_part.set("id", new_id)

    st_templates = new_part.findall("Staff")
    logger.debug("Removing extra staff templates", extra={"templates_kept": len(st_templates)})
    for st in st_templates[1:]:
        new_part.remove(st)

    idx = len(score)
    for i, el in enumerate(score):
        if el.tag == "Staff":
            idx = i
            break
    logger.debug("Inserting new part at index", extra={"insert_index": idx})
    score.insert(idx, new_part)

    new_staff = deepcopy(donor_staff)
    used_staff_ids.add(new_id)
    new_staff.set("id", new_id)
    logger.debug("Appending new staff with ID", extra={"staff_id": new_id})
    score.append(new_staff)

    if primary_staff is not None:
        logger.debug("Building placeholders from primary staff")
        placeholders = _build_placeholders_from_reference(primary_staff)
        logger.debug("Removing existing measures and adding placeholders", extra={"placeholders_count": len(placeholders)})
        for dm in list(new_staff):
            if dm.tag == "Measure":
                new_staff.remove(dm)
        for ph in placeholders:
            new_staff.append(ph)

    remove_hvboxes(new_staff)

    base_order = find_order(score)
    donor_order = find_order(get_score(donor_root))
    if base_order is not None and donor_order is not None:
        instr = donor_part.find(".//Instrument")
        instr_id = instr.get("id") if instr is not None and instr.get("id") else longName.lower().replace(" ", "_")
        assert isinstance(instr_id, str)
        exists = any(it.get("id") == instr_id for it in base_order.findall("instrument"))
        if not exists:
            logger.debug("Copying instrument into order", extra={"instrument_id": instr_id})
            copy_instrument_into_order(base_order, donor_order, instr_id)
    else:
        logger.warning("Could not locate <Order> in donor/base; instrument stub not copied.")

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
    logger.debug("Parsing XML", extra={"label": label, "size_bytes": len(b)})
    parser = ET.XMLParser()
    try:
        logger.debug("Attempting direct XML parse")
        return ET.parse(io.BytesIO(b), parser)
    except ET.ParseError as e:
        logger.debug("Direct parse failed, trying sanitized", extra={"error": str(e)})
        try:
            return ET.parse(io.BytesIO(sanitize(b)), parser)
        except ET.ParseError as e:
            logger.debug("Sanitized parse failed, trying truncated", extra={"error": str(e)})
            try:
                return ET.parse(io.BytesIO(truncate_root(sanitize(b))), parser)
            except ET.ParseError as e:
                logger.error("Could not parse donor", extra={"label": label, "error": str(e)})
                return None


# --------------------------
# Zip helpers
# --------------------------


def ensure_dir(path: str) -> None:
    if path:
        os.makedirs(path, exist_ok=True)


def write_zip_from_dir(src_dir: str, out_zip: str) -> None:
    logger.debug("Creating zip from directory", extra={"src_dir": src_dir, "out_zip": out_zip})
    ensure_dir(os.path.dirname(os.path.abspath(out_zip)) or ".")
    file_count = 0
    with zipfile.ZipFile(out_zip, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for root, _dirs, files in os.walk(src_dir):
            for fname in files:
                abs_path = os.path.join(root, fname)
                rel_path = os.path.relpath(abs_path, src_dir).replace(os.sep, "/")
                z.write(abs_path, rel_path)
                file_count += 1
    logger.debug("Added files to zip archive", extra={"file_count": file_count})


# --------------------------
# Main
# --------------------------


def main() -> int:
    ap = argparse.ArgumentParser(description="Merge MS4 .mscz files")
    ap.add_argument("-o", "--output-name", required=True)
    ap.add_argument("-D", "--output-dir", default=".")
    ap.add_argument("-N", "--new-page", action="store_true", help="start each merged score on a new page")
    ap.add_argument("file", nargs="+")
    args = ap.parse_args()

    rotate_log_file()

    logger.info("Starting merge process")
    logger.debug(
        "Parsed command line arguments",
        extra={
            "output_name": args.output_name,
            "output_dir": args.output_dir,
            "new_page": args.new_page,
            "files": args.file,
        },
    )

    base_zip = os.path.abspath(args.file[0])
    donor_list = [os.path.abspath(x) for x in args.file[1:]]
    out_dir = os.path.abspath(args.output_dir)
    out_path = os.path.join(out_dir, f"{args.output_name}.mscz")

    logger.debug("Base file", extra={"base_file": base_zip})
    logger.debug("Donor files", extra={"donor_files": donor_list})
    logger.debug("Output path", extra={"output_path": out_path})

    if len(donor_list) == 0 and os.path.splitext(base_zip)[1] != ".mscz":
        logger.debug("Reading donor list from file", extra={"list_file": base_zip})
        with open(base_zip, "r", encoding="utf-8") as f:
            donor_list = [
                os.path.abspath(os.path.join(".", line.strip())) for line in f.readlines() if len(line.strip()) > 0
            ]
            base_zip = donor_list[0]
            donor_list = donor_list[1:]
        logger.debug("Resolved files from list", extra={"base_file": base_zip, "donor_files": donor_list})

    logger.debug("Validating file existence")
    if not os.path.isfile(base_zip):
        logger.error("Base file not found", extra={"base_file": base_zip})
        return 66
    for d in donor_list:
        if not os.path.isfile(d):
            logger.error("Donor file not found", extra={"donor_file": d})
            return 66
    logger.debug("All input files validated successfully")

    work = tempfile.mkdtemp("mscore_merge_")
    base_dir = os.path.join(work, "base")
    ensure_dir(base_dir)
    logger.debug("Working directory", extra={"work_dir": work})

    try:
        logger.debug("Extracting base file", extra={"base_file": base_zip})
        with zipfile.ZipFile(base_zip, "r") as z:
            z.extractall(base_dir)
            mscx_name = first_mscx_name(z)
            if not mscx_name:
                logger.error("Base has no .mscx")
                return 65
            logger.debug("Base MSCX file", extra={"mscx_name": mscx_name})

        base_mscx = os.path.join(base_dir, mscx_name)
        logger.debug("Parsing base MSCX", extra={"base_mscx": base_mscx})
        raw = open(base_mscx, "rb").read()
        base_tree = ET.parse(io.BytesIO(raw))
        base_root = base_tree.getroot()
        logger.debug("Base XML parsed successfully")

        logger.debug("Indexing base score parts")
        base_name_to_part, base_names_order, base_name_to_staves, base_staff_by_id = index_single_staff_parts(
            base_root, base_mscx
        )
        used_staff_ids = set(base_staff_by_id.keys())
        logger.debug("Base parts indexed", extra={"base_names_order": base_names_order})
        logger.debug("Used staff IDs", extra={"used_staff_ids": list(used_staff_ids)})

        solo_base = [nm for nm in base_names_order if _is_solo_part(base_name_to_part[nm])]
        normal_base = [nm for nm in base_names_order if nm not in solo_base]
        keys_order = solo_base + normal_base
        logger.debug("Keys order", extra={"keys_order": keys_order})

        primary_staff = base_staff_by_id.get("1")

        logger.debug("Beginning donor processing", extra={"donor_count": len(donor_list)})
        for donor_path in donor_list:
            label = os.path.basename(donor_path)
            logger.info("Processing donor", extra={"donor_label": label})
            logger.debug("Donor path", extra={"donor_path": donor_path})
            try:
                logger.debug("Opening donor zip", extra={"donor_path": donor_path})
                with zipfile.ZipFile(donor_path, "r") as dz:
                    dn_mscx = first_mscx_name(dz)
                    if not dn_mscx:
                        logger.warning("No .mscx in donor, skipping", extra={"donor_label": label})
                        continue
                    logger.debug("Donor MSCX", extra={"donor_mscx": dn_mscx})
                    donor_bytes = dz.read(dn_mscx)
            except Exception as e:
                logger.error("Could not read donor, skipping", extra={"donor_label": label, "error": str(e)})
                continue

            donor_tree = parse_xml_lenient(donor_bytes, label)
            if donor_tree is None:
                continue
            donor_root = donor_tree.getroot()
            logger.debug("Donor XML parsed successfully", extra={"donor_label": label})

            logger.debug("Indexing donor score parts", extra={"donor_label": label})
            donor_name_to_part, donor_names_order, donor_name_to_staves, donor_staff_by_id = index_single_staff_parts(
                donor_root, donor_path
            )
            logger.debug("Donor parts indexed", extra={"donor_names_order": donor_names_order})

            logger.debug("Merging donor into base score", extra={"donor_label": label})
            score = get_score(base_root)

            donor_locks = get_score(donor_root).find("SystemLocks")
            if donor_locks is not None:
                donor_system_locks = donor_locks.findall("systemLock")
                base_systemlock = score.find("SystemLocks")
                logger.debug(
                    "Merging system locks from donor", extra={"system_locks_count": len(donor_system_locks) if donor_system_locks else 0, "donor_label": label}
                )

                if base_systemlock is not None and donor_system_locks is not None:
                    base_systemlock.extend(donor_system_locks)
                elif base_systemlock is None and donor_system_locks is not None:
                    score.append(deepcopy(donor_locks))

            logger.debug("Inserting breaks in base staves", extra={"donor_label": label})
            for staff in score.findall("Staff"):
                if "id" not in staff.attrib:
                    continue

                lm = last_measure(staff)
                if lm is None:
                    continue

                insert_break(lm, args.new_page)
                ensure_measure_end_barline(lm)

            donor_first_name = donor_names_order[0] if donor_names_order else None
            donor_first_staff = donor_name_to_staves.get(donor_first_name, [])[0] if donor_first_name else None

            base_names = set(keys_order)
            new_voices = [nm for nm in donor_names_order if nm not in base_names]
            logger.debug("New voices to create from donor", extra={"donor_label": label, "new_voices": new_voices})
            for nm in new_voices:
                logger.debug("Creating new voice", extra={"voice_name": nm})
                dp = donor_name_to_part[nm]
                ds = donor_name_to_staves[nm][0]

                new_part, new_staff = create_new_voice(
                    base_root,
                    donor_root,
                    nm,
                    dp,
                    ds,
                    used_staff_ids,
                    primary_staff,
                )

                base_name_to_staves[nm] = [new_staff]
                base_name_to_part[nm] = new_part
                keys_order.append(nm)

                solo_all = [n for n in keys_order if _is_solo_part(base_name_to_part[n])]
                normal_all = [n for n in keys_order if n not in solo_all]
                keys_order = solo_all + normal_all
                logger.debug("Updated keys order", extra={"keys_order": keys_order})

            donor_ref_staff = donor_first_staff
            donor_placeholders = []
            if donor_ref_staff is not None:
                donor_placeholders = _build_placeholders_from_reference(donor_ref_staff)
                logger.debug("Built placeholders from donor reference staff", extra={"placeholders_count": len(donor_placeholders)})

            logger.debug("Appending measures", extra={"donor_label": label})
            for nm in keys_order:
                base_staves = base_name_to_staves.get(nm)
                if base_staves is None:
                    continue

                if nm in donor_name_to_staves:
                    donor_staves = donor_name_to_staves[nm]
                    logger.debug("Appending content for existing voice", extra={"voice_name": nm})
                    for bs, ds in zip(base_staves, donor_staves):
                        measure_count = len([ch for ch in ds if ch.tag == "Measure"])
                        logger.debug("Appending measures to staff", extra={"measure_count": measure_count})
                        for ch in list(ds):
                            bs.append(deepcopy(ch))
                else:
                    logger.debug("Appending placeholders for voice", extra={"voice_name": nm})
                    for bs in base_staves:
                        for ph in donor_placeholders:
                            bs.append(deepcopy(ph))

        logger.debug("Beginning final score processing")
        score = get_score(base_root)
        logger.debug("Reordering staves to match parts (soloists first)")
        reorder_staves_to_match_parts_soloists_first(score)
        logger.debug("Renumbering staff IDs sequentially")
        renumber_staff_ids_sequential(score)
        logger.debug("Reordering parts in place")
        reorder_parts_inplace(score)
        logger.debug("Relocating H/V boxes to first staff")
        relocate_hvboxes_to_first_staff_by_measure_ordinal(score)
        logger.debug("Relocating system spanners to first staff")
        relocate_system_spanners_to_first_staff(score)
        logger.debug("Relocating system texts to first staff")
        relocate_system_texts_to_first_staff(score)
        logger.debug("Hiding empty voices")
        hide_empty_voices(score)

        logger.debug("Writing modified XML back to file")
        buf = io.BytesIO()
        logger.debug("Indenting XML tree")
        ET.indent(base_tree)
        logger.debug("Serializing XML to buffer")
        base_tree.write(buf, encoding="utf-8", xml_declaration=True)
        xml_size = len(buf.getvalue())
        logger.debug("XML serialization complete", extra={"xml_size_bytes": xml_size})
        open(base_mscx, "wb").write(buf.getvalue())

        logger.debug("Moving MSCX to base directory")
        shutil.move(base_mscx, os.path.join(base_dir, f"{args.output_name}.mscx"))
        logger.debug("Creating output zip archive")
        ensure_dir(out_dir)
        write_zip_from_dir(base_dir, out_path)
        logger.info("Merged archive created", extra={"output_path": out_path})

        logger.debug("Cleaning up temporary directory")
        shutil.rmtree(work)
        logger.debug("Merge process completed successfully")

    finally:
        logger.debug("Cleaning up temporary directory")
        shutil.rmtree(work, ignore_errors=True)

    return 0


if __name__ == "__main__":
    sys.exit(main())
