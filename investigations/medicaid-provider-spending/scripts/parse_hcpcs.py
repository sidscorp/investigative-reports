#!/usr/bin/env python3
"""
Parse the CMS HCPCS fixed-width file into a clean CSV.
Based on the record layout in HCPC2026_recordlayout.txt.
"""

import csv
from pathlib import Path


def parse_hcpcs(input_path: str, output_path: str):
    """
    Parse fixed-width HCPCS file into CSV.

    Record layout (key fields):
      - HCPCS Code:          pos 1-5   (5 chars)
      - Modifier Code:       pos 4-5   (2 chars, redefine of last 2 of HCPCS code)
      - Sequence Number:     pos 6-10  (5 chars)
      - Record ID Code:      pos 11    (1 char: 3=first procedure line, 4=continuation,
                                                 7=first modifier line, 8=continuation)
      - Long Description:    pos 12-91 (80 chars)
      - Short Description:   pos 92-119 (28 chars)
    """
    print(f"Parsing: {input_path}")

    codes = {}  # code -> {short_desc, long_desc}

    with open(input_path, 'r', encoding='latin-1') as f:
        for line_num, line in enumerate(f, 1):
            if len(line.rstrip('\n')) < 91:
                continue

            hcpcs_code = line[0:5].strip()
            record_id = line[10:11].strip()
            long_desc = line[11:91].strip()
            short_desc = line[91:119].strip() if len(line) >= 119 else ""

            if not hcpcs_code:
                continue

            # Record ID 3 = first line of procedure, 7 = first line of modifier
            if record_id in ('3', '7'):
                codes[hcpcs_code] = {
                    'short_desc': short_desc,
                    'long_desc': long_desc
                }
            elif record_id in ('4', '8'):
                # Continuation line - append to long description
                if hcpcs_code in codes:
                    codes[hcpcs_code]['long_desc'] += ' ' + long_desc

    # Write CSV
    with open(output_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['HCPCS_CODE', 'SHORT_DESCRIPTION', 'LONG_DESCRIPTION'])
        for code in sorted(codes.keys()):
            writer.writerow([
                code,
                codes[code]['short_desc'],
                codes[code]['long_desc']
            ])

    print(f"Wrote {len(codes):,} HCPCS codes to: {output_path}")


if __name__ == "__main__":
    data_dir = Path(__file__).resolve().parent.parent / "data"
    input_file = str(data_dir / "hcpcs" / "HCPC2026_JAN_ANWEB_01122026.txt")
    output_file = str(data_dir / "hcpcs_codes.csv")

    parse_hcpcs(input_file, output_file)
