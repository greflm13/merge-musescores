# MergeMuseScores

Merge multiple MuseScore 4 (`.mscz`) scores into a single conductor-style score while automatically handling staff alignment, missing instruments, section breaks, page breaks, title pages, and table of contents generation.

## Features

- Merge multiple MuseScore 4 scores into a single `.mscz` file
- Automatically adds missing instruments from donor scores
- Creates placeholder measures for instruments that are not present in every score
- Preserves:
  - Time signatures
  - Key signatures
  - Layout breaks
  - System texts
  - Tempo markings
  - Voltas and system spanners
- Automatically hides empty staves
- Optional page break between merged scores
- Optional automatically generated Table of Contents
- Optional title page
- Handles malformed MSCX files using lenient XML parsing
- Reorders soloists to the top of the score
- Renumbers staff IDs to maintain MuseScore consistency
- Preserves score metadata and updates output metadata

## Requirements

- Python 3.11+
- MuseScore 4 score files (`.mscz`)

## Installation

```bash
git clone https://github.com/greflm13/merge-musescores.git
cd merge-musescores
pip install .
```

## Usage

```txt
usage: merge-scores [-h] -o OUTPUT_NAME [-D OUTPUT_DIR] [-N] [-T] [--toc-name TOC_NAME] [-P] [-V] file [file ...]

Merge MS4 .mscz files

positional arguments:
file

options:
-h, --help            show this help message and exit
-o OUTPUT_NAME, --output-name OUTPUT_NAME
-D OUTPUT_DIR, --output-dir OUTPUT_DIR
-N, --new-page        start each merged score on a new page
-T, --table-of-contents
                      add table of contents on first page
--toc-name TOC_NAME   ToC Title, (default 'Table of Contents')
-P, --title-page      add title page
-V, --version         show program's version number and exit
```

## Input File Lists

Instead of specifying every score on the command line, you may provide a text file:

```txt
Song1.mscz
Song2.mscz
Song3.mscz
Song4.mscz
```

Then run:

```bash
merge-scores \
-o ConcertBook \
scores.txt
```

The first score in the list becomes the base score.

## Limitations

Designed for MuseScore 4 scores.

Assumes single-staff parts when matching instruments.

Instrument matching relies on unique longName values.

Page numbers in generated tables of contents are based on song order rather than final
rendered MuseScore layout.

## License

See the project's LICENSE file for details.
