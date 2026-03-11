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
    for ln in part.findall(".//Instrument/longName"):
        val = (ln.text or "").strip()
        if val:
            return val
    ln = part.find("longName")
    if ln is not None and (ln.text or "").strip():
        return (ln.text or "").strip()
    return None


def index_single_staff_parts(root: ET.Element, file: str):

    score = get_score(root)
    parts = list(score.findall("Part"))

    name_to_part: Dict[str, ET.Element] = {}
    name_list: List[str] = []

    for p in parts:
        name = extract_longname(p)

        if not name:
            eprint(f"Warning: Part without longName skipped. {file}")
            continue

        if name in name_to_part:
            eprint(f"Warning: Duplicate longName '{name}' skipped. {file}")
            continue

        name_to_part[name] = p
        name_list.append(name)

    score_staves = [s for s in score.findall("Staff") if "id" in s.attrib]

    name_to_staves: Dict[str, List[ET.Element]] = {}

    cursor = 0

    for name in name_list:
        part = name_to_part[name]

        templates = part.findall("Staff")
        count = len(templates)

        if cursor + count > len(score_staves):
            eprint(f"Warning: staff mismatch. {file}")
            break

        name_to_staves[name] = score_staves[cursor : cursor + count]

        cursor += count

    staff_by_id = {s.attrib["id"]: s for s in score_staves}

    final_names = [nm for nm in name_list if nm in name_to_staves]

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


def strip_measure(measure: ET.Element, timesig: str):
    voices = measure.findall("voice")
    measure_len = measure.get("len")
    if measure_len:
        timesig = measure_len
    if voices is None:
        return
    voice = voices[0]
    removed = True
    while removed:
        removed = False
        for parent in list(voice.iter()):
            for ch in list(parent):
                if ch.tag not in NOTE_TAGS:
                    parent.remove(ch)
                    removed = True

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


def hide_empty_voices(score: ET.Element):
    he = ET.Element("hideWhenEmpty")
    he.text = "on"
    parts = score.findall("Part")
    for part in list(parts):
        if part.find("hideWhenEmpty") is None:
            part.append(he)


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


def reorder_parts_inplace(score: ET.Element):
    """
    Reorder <Part> elements IN PLACE so all soloists are first.
    Does NOT touch <Staff>. Uses remove+insert for correct moves.
    """

    # Current parts in document order
    parts = [el for el in score if el.tag == "Part"]

    solo = [p for p in parts if _is_solo_part(p)]
    non = [p for p in parts if not _is_solo_part(p)]
    desired = solo + non

    # First collect all <Part> positions in the tree
    children = list(score)
    part_positions = [i for i, ch in enumerate(children) if ch.tag == "Part"]

    pos_iter = iter(part_positions)

    for target_part in desired:
        try:
            target_index = next(pos_iter)
        except StopIteration:
            break

        # If it is already in the correct spot, skip
        if score[target_index] is target_part:
            continue

        # Correct, safe move:
        #   1. Remove this specific part from wherever it is now
        score.remove(target_part)
        #   2. Insert at the correct target index
        score.insert(target_index, target_part)

        # Refresh children snapshot only for indices
        children = list(score)


def _is_solo_part(p: ET.Element) -> bool:
    sol = p.find("soloist")
    return sol is not None and (sol.text or "").strip() == "1"


def _collect_parts(score: ET.Element) -> list[ET.Element]:
    # Score-level <Part> elements, in document order
    return [el for el in score if el.tag == "Part"]


def _collect_score_staves(score: ET.Element) -> list[ET.Element]:
    # Score-level <Staff> elements (not the templates under <Part>), in document order
    return [el for el in score if el.tag == "Staff" and "id" in el.attrib]


def compute_soloist_permutation_from_current_parts(score: ET.Element) -> list[int]:
    """
    Compute a permutation of indices for the *current* Parts so that soloists come first.
    We only read current XML state; no reliance on name_to_* maps.
    """
    parts = _collect_parts(score)
    solo_idx = [i for i, p in enumerate(parts) if _is_solo_part(p)]
    non_idx = [i for i in range(len(parts)) if i not in solo_idx]
    return solo_idx + non_idx


def _reorder_block_inplace_by_permutation(score: ET.Element, tag: str, base_elems: list[ET.Element], perm: list[int]):
    """
    In-place reorder of a homogeneous block (<Staff> here) to follow a permutation that
    was computed on the *Parts*. 'base_elems' must be a snapshot of the block BEFORE any moves.
    We use remove+insert for a single node at a time to avoid duplicates.
    """
    # Current linear positions of the target tag inside score
    children = list(score)
    tag_positions = [i for i, ch in enumerate(children) if ch.tag == tag]

    # Desired order by applying perm to the pre-move snapshot
    desired = [base_elems[i] for i in perm if i < len(base_elems)]

    pos_iter = iter(tag_positions)
    for node in desired:
        try:
            target_pos = next(pos_iter)
        except StopIteration:
            break
        if score[target_pos] is node:
            continue
        score.remove(node)
        score.insert(target_pos, node)


def reorder_staves_to_match_parts_soloists_first(score: ET.Element):
    """
    Compute soloist-first permutation from current Parts and mirror that onto
    score-level Staves (single-staff mode). Does NOT touch Part order or IDs.
    """
    parts_snapshot = _collect_parts(score)
    staves_snapshot = _collect_score_staves(score)
    if not parts_snapshot or not staves_snapshot:
        return

    perm = compute_soloist_permutation_from_current_parts(score)
    # Mirror permutation onto staves using the pre-move snapshot
    _reorder_block_inplace_by_permutation(score, "Staff", staves_snapshot, perm)


def renumber_staff_ids_sequential(score: ET.Element):
    """
    After reordering staves, set only <Staff id> to 1..N (in their current order).
    Does NOT touch <Part id>.
    """
    staves = _collect_score_staves(score)
    for i, st in enumerate(staves, start=1):
        st.set("id", str(i))


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


def measure_index_of_node(staff: ET.Element, node: ET.Element) -> int:
    count = 0
    for ch in staff:
        if ch is node:
            break
        if ch.tag == "Measure":
            count += 1
    return count


def insert_before_measure_ordinal(first_staff: ET.Element, m: int, vb: ET.Element):
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


def relocate_vboxes_to_first_staff_by_measure_ordinal(score: ET.Element):
    """
    Collect all VBoxes from all score-level staves and reinsert them into the
    final first staff (current score order), preserving *measure ordinal*:
      - If a VBox had k measures before it on its original staff, insert it
        before the k-th measure of the first staff (k=0 => before first measure).
      - If k >= number of measures in the first staff, append at the end.

    Rationale:
      - During donor append you keep VBoxes on the same staff as their donor content,
        so mid-section VBoxes stay adjacent to the correct measure boundary.
      - After all merges and any reorders/renumbering, you run this pass once to move
        every VBox onto staff 1 (so they are visible) without drifting by one measure.
    """
    # Score-level staves, in current (final) order
    staves = [el for el in score if el.tag == "Staff" and "id" in el.attrib]
    if not staves:
        return
    first_staff = staves[0]

    vboxes = []
    for staff in staves:
        for ch in list(staff):
            if ch.tag == "VBox":
                vboxes.append((measure_index_of_node(staff, ch), ch, staff))

    if not vboxes:
        return

    for _, vb, src_staff in vboxes:
        try:
            src_staff.remove(vb)
        except Exception:
            pass

    vboxes_sorted = sorted(enumerate(vboxes), key=lambda t: (t[1][0], t[0]))
    for _, (m_idx, vb, _src) in vboxes_sorted:
        insert_before_measure_ordinal(first_staff, m_idx, vb)


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
    current_ts = ""
    for m in ref_ms:
        if "len" in m.attrib:
            ts = m.attrib["len"]
        else:
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
    new_part = deepcopy(donor_part)
    new_id = next_id(used_staff_ids)
    new_part.set("id", new_id)

    st_templates = new_part.findall("Staff")
    for st in st_templates[1:]:
        new_part.remove(st)

    idx = len(score)
    for i, el in enumerate(score):
        if el.tag == "Staff":
            idx = i
            break
    score.insert(idx, new_part)

    new_staff = deepcopy(donor_staff)
    used_staff_ids.add(new_id)
    new_staff.set("id", new_id)
    score.append(new_staff)

    if primary_staff is not None:
        placeholders = _build_placeholders_from_reference(primary_staff)
        for dm in list(new_staff):
            if dm.tag == "Measure":
                new_staff.remove(dm)
        for ph in placeholders:
            new_staff.append(ph)

    remove_vboxes(new_staff)

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
    ap.add_argument("file", nargs="+")
    args = ap.parse_args()

    base_zip = os.path.abspath(args.file[0])
    donor_list = [os.path.abspath(x) for x in args.file[1:]]
    out_dir = os.path.abspath(args.output_dir)
    out_path = os.path.join(out_dir, f"{args.output_name}.mscz")

    if len(donor_list) == 0 and os.path.splitext(base_zip)[1] != ".mscz":
        with open(base_zip, "r", encoding="utf-8") as f:
            donor_list = [
                os.path.abspath(os.path.join(".", line.strip())) for line in f.readlines() if len(line.strip()) > 0
            ]
            base_zip = donor_list[0]
            donor_list = donor_list[1:]
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

        base_name_to_part, base_names_order, base_name_to_staves, base_staff_by_id = index_single_staff_parts(
            base_root, base_mscx
        )
        used_staff_ids = set(base_staff_by_id.keys())

        solo_base = [nm for nm in base_names_order if _is_solo_part(base_name_to_part[nm])]
        normal_base = [nm for nm in base_names_order if nm not in solo_base]
        keys_order = solo_base + normal_base

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

            donor_name_to_part, donor_names_order, donor_name_to_staves, donor_staff_by_id = index_single_staff_parts(
                donor_root, donor_path
            )

            donor_locks = get_score(donor_root).find("SystemLocks")
            if donor_locks is not None:
                system_locks = donor_locks.findall("systemLock")
                systemlock = get_score(base_root).find("SystemLocks")

                if systemlock is not None and system_locks is not None:
                    systemlock.extend(system_locks)

            if primary_staff is not None:
                lm = last_measure(primary_staff)
                if lm is not None:
                    insert_break(lm)

            donor_first_name = donor_names_order[0] if donor_names_order else None
            donor_first_staff = donor_name_to_staves.get(donor_first_name, [])[0] if donor_first_name else None

            base_names = set(keys_order)
            new_voices = [nm for nm in donor_names_order if nm not in base_names]
            for nm in new_voices:
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

            donor_ref_staff = donor_first_staff
            donor_placeholders = []
            if donor_ref_staff is not None:
                donor_placeholders = _build_placeholders_from_reference(donor_ref_staff)

            for nm in keys_order:
                base_staves = base_name_to_staves.get(nm)
                if base_staves is None:
                    continue

                if nm in donor_name_to_staves:
                    donor_staves = donor_name_to_staves[nm]
                    for bs, ds in zip(base_staves, donor_staves):
                        for ch in list(ds):
                            if ch.tag == "VBox":
                                if primary_staff is not None:
                                    primary_staff.append(deepcopy(ch))
                                continue
                            bs.append(deepcopy(ch))
                else:
                    for bs in base_staves:
                        for ph in donor_placeholders:
                            bs.append(deepcopy(ph))

        score = get_score(base_root)
        reorder_staves_to_match_parts_soloists_first(score)
        renumber_staff_ids_sequential(score)
        reorder_parts_inplace(score)
        relocate_vboxes_to_first_staff_by_measure_ordinal(score)
        hide_empty_voices(score)

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
