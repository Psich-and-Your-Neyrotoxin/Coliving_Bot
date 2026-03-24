from __future__ import annotations

from collections import OrderedDict, defaultdict
from dataclasses import dataclass
from datetime import date
from io import BytesIO
import json
from pathlib import Path
from xml.sax.saxutils import escape
from zipfile import ZIP_DEFLATED, ZipFile

from handlers.common import canonical_full_name


MONTH_NAMES = {
    1: "Січень",
    2: "Лютий",
    3: "Березень",
    4: "Квітень",
    5: "Травень",
    6: "Червень",
    7: "Липень",
    8: "Серпень",
    9: "Вересень",
    10: "Жовтень",
    11: "Листопад",
    12: "Грудень",
}

ZONE_LABELS = {
    "Кухня": "Кухня",
    "Ванна": "Ванни",
    "Общак": "Общак",
}

TITLE_STYLE = 1
NAME_STYLE = 2
HEADER_STYLE = 3
CELL_STYLE = 4
MARK_STYLE = 5

# Excel column width is stored in character units.
# These values keep the sheet compact but readable without manual resizing.
DAY_COLUMN_WIDTH = 4.2
NAME_COLUMN_WIDTH = 26
RESIDENTS_JSON_PATH = Path(__file__).with_name("residents.json")


@dataclass(frozen=True)
class Record:
    day: date
    zone: str
    person: str
    status: str


@dataclass(frozen=True)
class Cell:
    row: int
    col: int
    style: int
    value: str = ""


def ordered_people(records: list[Record], zone: str) -> list[str]:
    seen: OrderedDict[str, None] = OrderedDict()
    for record in sorted(records, key=lambda item: (item.day, item.person)):
        if record.zone == zone:
            seen.setdefault(record.person, None)

    def order_aliases(name: str) -> set[str]:
        raw_parts = [part for part in str(name).split() if part]
        aliases = {" ".join(raw_parts).strip().lower()}
        canonical = canonical_full_name(name)
        canonical_parts = [part for part in canonical.split() if part]
        aliases.add(" ".join(canonical_parts).strip().lower())
        if len(raw_parts) >= 2:
            aliases.add(f"{raw_parts[1]} {raw_parts[0]}".strip().lower())
        if len(canonical_parts) >= 2:
            aliases.add(f"{canonical_parts[1]} {canonical_parts[0]}".strip().lower())
        return {alias for alias in aliases if alias}

    preferred_order: dict[str, tuple[int, int]] = {}
    try:
        residents = json.loads(RESIDENTS_JSON_PATH.read_text(encoding="utf-8"))
        if isinstance(residents, list):
            for index, resident in enumerate(residents):
                name = str(resident.get("full_name", "")).strip()
                if not name:
                    continue

                if zone == "Кухня" and resident.get("kitchen_order") is not None:
                    order_value = (int(resident["kitchen_order"]), 0)
                elif zone == "Ванна" and resident.get("bath_order") is not None:
                    order_value = (int(resident["bath_order"]), 0)
                elif zone == "Общак" and resident.get("general_pair_order") is not None:
                    pair_order = int(resident["general_pair_order"])
                    pair_slot = int(resident.get("general_pair_slot", 1))
                    order_value = (pair_order, pair_slot)
                else:
                    order_value = (10_000 + index, 0)

                for alias in order_aliases(name):
                    preferred_order[alias] = order_value
    except Exception:
        preferred_order = {}

    return sorted(
        seen.keys(),
        key=lambda name: (
            min((preferred_order.get(alias, (10_000, 0)) for alias in order_aliases(name)), default=(10_000, 0)),
            name,
        ),
    )


def build_kitchen_tables(records: list[Record]) -> list[dict]:
    kitchen = [record for record in records if record.zone == "Кухня"]
    people = ordered_people(records, "Кухня")

    by_month: dict[tuple[int, int], dict[int, str]] = defaultdict(dict)
    for record in kitchen:
        by_month[(record.day.year, record.day.month)][record.day.day] = record.person

    tables = []
    for (year, month) in sorted(by_month):
        days = sorted(by_month[(year, month)].keys())
        tables.append(
            {
                "title": f"Кухня - {MONTH_NAMES[month]} {year}",
                "headers": [str(day) for day in days],
                "people": people,
                "marks": {
                    person: {
                        str(day): by_month[(year, month)].get(day) == person for day in days
                    }
                    for person in people
                },
            }
        )
    return tables


def build_weekly_table(records: list[Record], zone: str) -> dict:
    zone_records = [record for record in records if record.zone == zone]
    people = ordered_people(records, zone)

    weeks: OrderedDict[int, list[str]] = OrderedDict()
    for record in sorted(zone_records, key=lambda item: (item.day, item.person)):
        week = record.day.isocalendar().week
        week_people = weeks.setdefault(week, [])
        if record.person not in week_people:
            week_people.append(record.person)

    headers = [str(week) for week in weeks]
    marks = {
        person: {header: person in weeks[int(header)] for header in headers}
        for person in people
    }

    return {
        "title": ZONE_LABELS.get(zone, zone),
        "headers": headers,
        "people": people,
        "marks": marks,
    }


def build_tables(records: list[Record]) -> list[dict]:
    tables = build_kitchen_tables(records)
    for zone in ("Ванна", "Общак"):
        tables.append(build_weekly_table(records, zone))
    return tables


def build_sheet_layout(tables: list[dict]) -> tuple[list[Cell], list[str], int, int]:
    cells: list[Cell] = []
    merges: list[str] = []
    current_row = 1
    max_col = 1

    for table in tables:
        last_col = len(table["headers"]) + 1
        max_col = max(max_col, last_col)

        cells.append(Cell(current_row, 1, TITLE_STYLE, table["title"]))
        for col in range(2, last_col + 1):
            cells.append(Cell(current_row, col, TITLE_STYLE))
        merges.append(f"{cell_ref(current_row, 1)}:{cell_ref(current_row, last_col)}")
        current_row += 1

        cells.append(Cell(current_row, 1, HEADER_STYLE))
        for offset, header in enumerate(table["headers"], start=2):
            cells.append(Cell(current_row, offset, HEADER_STYLE, header))
        current_row += 1

        for person in table["people"]:
            cells.append(Cell(current_row, 1, NAME_STYLE, person))
            for offset, header in enumerate(table["headers"], start=2):
                is_marked = table["marks"][person].get(header, False)
                style = MARK_STYLE if is_marked else CELL_STYLE
                cells.append(Cell(current_row, offset, style))
            current_row += 1

        current_row += 2

    return cells, merges, current_row, max_col


def cell_ref(row: int, col: int) -> str:
    return f"{col_letters(col)}{row}"


def col_letters(col: int) -> str:
    letters: list[str] = []
    while col:
        col, rem = divmod(col - 1, 26)
        letters.append(chr(65 + rem))
    return "".join(reversed(letters))


def xml_declared(content: str) -> str:
    return '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' + content


def make_cell_xml(cell: Cell) -> str:
    ref = cell_ref(cell.row, cell.col)
    if cell.value:
        return (
            f'<c r="{ref}" s="{cell.style}" t="inlineStr">'
            f"<is><t>{escape(cell.value)}</t></is></c>"
        )
    return f'<c r="{ref}" s="{cell.style}"/>'


def build_sheet_xml(cells: list[Cell], merges: list[str], max_row: int, max_col: int) -> str:
    rows: dict[int, list[Cell]] = defaultdict(list)
    for cell in cells:
        rows[cell.row].append(cell)

    row_chunks: list[str] = []
    for row_number in sorted(rows):
        ordered = sorted(rows[row_number], key=lambda item: item.col)
        row_xml = "".join(make_cell_xml(cell) for cell in ordered)
        row_chunks.append(f'<row r="{row_number}" ht="18" customHeight="1">{row_xml}</row>')

    merge_xml = ""
    if merges:
        merge_tags = "".join(f'<mergeCell ref="{ref}"/>' for ref in merges)
        merge_xml = f'<mergeCells count="{len(merges)}">{merge_tags}</mergeCells>'

    cols_xml = (
        "<cols>"
        f'<col min="1" max="1" width="{NAME_COLUMN_WIDTH}" customWidth="1"/>'
        f'<col min="2" max="{max_col}" width="{DAY_COLUMN_WIDTH}" customWidth="1"/>'
        "</cols>"
    )

    return xml_declared(
        f"""
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <dimension ref="A1:{cell_ref(max_row, max_col)}"/>
  <sheetViews>
    <sheetView workbookViewId="0"/>
  </sheetViews>
  <sheetFormatPr defaultRowHeight="18"/>
  {cols_xml}
  <sheetData>{''.join(row_chunks)}</sheetData>
  {merge_xml}
</worksheet>
""".strip()
    )


def build_styles_xml() -> str:
    return xml_declared(
        """
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <fonts count="2">
    <font>
      <sz val="11"/>
      <name val="Calibri"/>
    </font>
    <font>
      <b/>
      <sz val="11"/>
      <name val="Calibri"/>
    </font>
  </fonts>
  <fills count="4">
    <fill><patternFill patternType="none"/></fill>
    <fill><patternFill patternType="gray125"/></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FFF0AD7A"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FFFF1D14"/><bgColor indexed="64"/></patternFill></fill>
  </fills>
  <borders count="3">
    <border>
      <left/><right/><top/><bottom/><diagonal/>
    </border>
    <border>
      <left style="thin"><color rgb="FF000000"/></left>
      <right style="thin"><color rgb="FF000000"/></right>
      <top style="thin"><color rgb="FF000000"/></top>
      <bottom style="thin"><color rgb="FF000000"/></bottom>
      <diagonal/>
    </border>
    <border>
      <left style="medium"><color rgb="FF000000"/></left>
      <right style="medium"><color rgb="FF000000"/></right>
      <top style="medium"><color rgb="FF000000"/></top>
      <bottom style="medium"><color rgb="FF000000"/></bottom>
      <diagonal/>
    </border>
  </borders>
  <cellStyleXfs count="1">
    <xf numFmtId="0" fontId="0" fillId="0" borderId="0"/>
  </cellStyleXfs>
  <cellXfs count="6">
    <xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>
    <xf numFmtId="0" fontId="1" fillId="2" borderId="2" xfId="0" applyFont="1" applyFill="1" applyBorder="1" applyAlignment="1">
      <alignment horizontal="center" vertical="center"/>
    </xf>
    <xf numFmtId="0" fontId="0" fillId="0" borderId="1" xfId="0" applyBorder="1" applyAlignment="1">
      <alignment horizontal="left" vertical="center"/>
    </xf>
    <xf numFmtId="0" fontId="0" fillId="0" borderId="1" xfId="0" applyBorder="1" applyAlignment="1">
      <alignment horizontal="center" vertical="center"/>
    </xf>
    <xf numFmtId="0" fontId="0" fillId="0" borderId="1" xfId="0" applyBorder="1"/>
    <xf numFmtId="0" fontId="0" fillId="3" borderId="1" xfId="0" applyFill="1" applyBorder="1"/>
  </cellXfs>
  <cellStyles count="1">
    <cellStyle name="Normal" xfId="0" builtinId="0"/>
  </cellStyles>
</styleSheet>
""".strip()
    )


def build_workbook_xml(title: str) -> str:
    safe_title = title[:31] or "Розклад"
    return xml_declared(
        f"""
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"
          xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets>
    <sheet name="{escape(safe_title)}" sheetId="1" r:id="rId1"/>
  </sheets>
</workbook>
""".strip()
    )


def build_workbook_rels_xml() -> str:
    return xml_declared(
        """
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1"
                Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet"
                Target="worksheets/sheet1.xml"/>
  <Relationship Id="rId2"
                Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles"
                Target="styles.xml"/>
</Relationships>
""".strip()
    )


def build_root_rels_xml() -> str:
    return xml_declared(
        """
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1"
                Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument"
                Target="xl/workbook.xml"/>
</Relationships>
""".strip()
    )


def build_content_types_xml() -> str:
    return xml_declared(
        """
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml"
            ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml"
            ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
  <Override PartName="/xl/styles.xml"
            ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>
</Types>
""".strip()
    )


def build_xlsx_bytes(records: list[Record], title: str = "Розклад чергувань") -> bytes:
    tables = build_tables(records)
    cells, merges, max_row, max_col = build_sheet_layout(tables)

    buffer = BytesIO()
    with ZipFile(buffer, "w", compression=ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", build_content_types_xml())
        archive.writestr("_rels/.rels", build_root_rels_xml())
        archive.writestr("xl/workbook.xml", build_workbook_xml(title))
        archive.writestr("xl/_rels/workbook.xml.rels", build_workbook_rels_xml())
        archive.writestr("xl/styles.xml", build_styles_xml())
        archive.writestr(
            "xl/worksheets/sheet1.xml",
            build_sheet_xml(cells, merges, max_row, max_col),
        )
    return buffer.getvalue()
