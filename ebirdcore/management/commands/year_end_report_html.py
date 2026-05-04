# encoding: utf-8
"""
HTML version of the Annual eBird Statistical Report.
Usage: python manage.py year_end_report_html -r US-DC-001 -y 2024
"""

import html as _html
import logging

import requests
from django.core.management.base import BaseCommand

from ebirdcore.dc_ward_wkv import dc_ward_wkv
from ebirdcore.models import EBird
from ebirdcore.management.commands.year_end_report import Command as Queries

logger = logging.getLogger(__name__)


# ── Helpers ───────────────────────────────────────────────────────────────────


def h(s):
    if s is None:
        return ""
    return _html.escape(str(s))


# ── CSS ───────────────────────────────────────────────────────────────────────

CSS = r"""
:root {
    --green-dark: #1b4332;
    --green-mid: #2d6a4f;
    --green-light: #52b788;
    --amber: #d4a017;
    --bg: #eef4f0;
    --card: #ffffff;
    --text: #1a1a1a;
    --muted: #5a6474;
    --border: #d0ddd4;
    --stripe: #f2f9f4;
    --hover: #d8f0e4;
    --shadow: 0 2px 10px rgba(0,0,0,.08);
    --r: 8px;
}
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
body {
    font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;
    background: var(--bg);
    color: var(--text);
    line-height: 1.5;
}
a { color: var(--green-mid); }

/* Cover */
.cover {
    background: linear-gradient(150deg, var(--green-dark) 0%, #40916c 100%);
    color: #fff;
    padding: 3rem 2rem 2.5rem;
    text-align: center;
}
.cover h1 { font-size: 2rem; line-height: 1.3; text-shadow: 0 1px 4px rgba(0,0,0,.3); }
.cover .region { font-size: 1.1rem; opacity: .85; margin-top: .4rem; }
.cover .year-ver { font-size: .9rem; opacity: .65; margin-top: .2rem; }
.cover-photo {
    max-width: 560px; width: 100%; border-radius: var(--r);
    margin: 1.5rem auto .6rem; display: block;
    box-shadow: 0 4px 24px rgba(0,0,0,.4);
}
.cover-caption { font-size: .78rem; opacity: .72; max-width: 560px; margin: 0 auto; }

/* Page layout */
.layout {
    display: flex; max-width: 1440px; margin: 0 auto;
    padding: 1.5rem 1rem; gap: 1.5rem; align-items: flex-start;
}

/* Sidebar TOC */
nav.toc {
    width: 215px; flex-shrink: 0; position: sticky; top: 1rem;
    background: var(--card); border-radius: var(--r);
    padding: .9rem 1rem; box-shadow: var(--shadow);
    max-height: calc(100vh - 2rem); overflow-y: auto;
    font-size: .78rem;
}
nav.toc h3 {
    text-transform: uppercase; letter-spacing: .07em;
    font-size: .65rem; color: var(--muted); margin-bottom: .5rem;
}
nav.toc ol { padding-left: 1.1rem; }
nav.toc li { margin: .22rem 0; line-height: 1.3; }
nav.toc a { color: var(--green-mid); text-decoration: none; }
nav.toc a:hover { text-decoration: underline; }

/* Main content */
main { flex: 1; min-width: 0; }

/* Sections */
.sec {
    background: var(--card); border-radius: var(--r);
    padding: 1.4rem 1.5rem 1.2rem; margin-bottom: 1.4rem;
    box-shadow: var(--shadow);
}
.sec > h2 {
    font-size: 1.3rem; color: var(--green-dark);
    border-bottom: 2px solid var(--green-light);
    padding-bottom: .35rem; margin-bottom: .9rem;
}
.sec h3 {
    font-size: 1rem; color: var(--green-mid);
    margin: 1.2rem 0 .5rem;
}
.sec h3:first-child { margin-top: 0; }
p.desc { color: var(--muted); font-size: .875rem; margin-bottom: .9rem; line-height: 1.6; }

/* Multi-column table grids */
.tgrid { display: flex; flex-wrap: wrap; gap: .9rem; align-items: flex-start; }
.tgrid.c1 .tc { flex: 1 1 100%; }
.tgrid.c2 .tc { flex: 1 1 calc(50% - .45rem); min-width: 185px; }
.tgrid.c3 .tc { flex: 1 1 calc(33.3% - .6rem); min-width: 155px; }
.tc {}
.tc-title { font-weight: 700; font-size: .82rem; color: var(--green-dark); margin-bottom: .25rem; }

/* Data tables */
table.dt { width: 100%; border-collapse: collapse; font-size: .8rem; }
table.dt thead th {
    background: var(--green-dark); color: #fff;
    padding: .32rem .5rem; text-align: left;
    font-weight: 600; white-space: nowrap;
}
table.dt thead th:not(:first-child) { text-align: right; }
table.dt tbody td {
    padding: .26rem .5rem; border-bottom: 1px solid #eaeaea; vertical-align: top;
}
table.dt tbody td:not(:first-child) { text-align: right; white-space: nowrap; }
table.dt tbody tr:nth-child(even) { background: var(--stripe); }
table.dt tbody tr:hover { background: var(--hover); }
table.dt tr.sep td { border-top: 2px solid var(--border); }
table.dt a { color: inherit; }
table.dt td.loc { max-width: 160px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }

/* Photo grid */
.photo-grid {
    display: grid; grid-template-columns: repeat(3, 1fr);
    gap: .75rem; margin-top: .75rem;
}
.photo-item img {
    width: 100%; max-height: 210px; object-fit: contain;
    border-radius: 5px; display: block; background: var(--stripe);
    cursor: zoom-in; transition: opacity .15s;
}
.photo-item img:hover { opacity: .85; }
.photo-item p { font-size: .73rem; color: var(--muted); margin-top: .25rem; line-height: 1.3; }

/* Lightbox */
#lb {
    display: none; position: fixed; inset: 0;
    background: rgba(0,0,0,.88); z-index: 9999;
    align-items: center; justify-content: center; cursor: zoom-out;
}
#lb.open { display: flex; }
#lb img { max-width: 92vw; max-height: 92vh; border-radius: var(--r); box-shadow: 0 4px 40px rgba(0,0,0,.6); object-fit: contain; }

/* List sections */
.list-grid { columns: 3; column-gap: 1rem; }
.li { break-inside: avoid; padding: .1rem 0; font-size: .83rem; line-height: 1.4; }
.li .sub { color: var(--muted); font-size: .73rem; font-style: italic; display: block; }

/* Responsive */
@media (max-width: 1050px) { nav.toc { display: none; } }
@media (max-width: 720px) {
    .tgrid.c2 .tc, .tgrid.c3 .tc { flex: 1 1 100%; }
    .photo-grid { grid-template-columns: repeat(2, 1fr); }
    .list-grid { columns: 2; }
}
@media (max-width: 480px) {
    .cover h1 { font-size: 1.45rem; }
    .photo-grid { grid-template-columns: 1fr; }
    .list-grid { columns: 1; }
    .sec { padding: 1rem; }
}
"""


# ── Rendering helpers ─────────────────────────────────────────────────────────


def _render_table(column_desc, data, rank_by_colidx=-1, hline_every=5):
    vis = [(i, d) for i, d in enumerate(column_desc) if not d.name.startswith("_")]

    # Build map: visible column index -> raw column index of its URL
    # "_Url"  -> links visible col 0 (legacy)
    # "_UrlN" -> links visible col N
    url_for_col = {}
    for i, d in enumerate(column_desc):
        if d.name == "_Url":
            url_for_col[0] = i
        elif d.name.startswith("_Url") and d.name[4:].isdigit():
            url_for_col[int(d.name[4:])] = i

    headers = [("Δ" if d.name == "Chg" else d.name) for _, d in vis]
    th_html = "".join(f"<th>{h(hdr)}</th>" for hdr in headers)

    do_rank = (
        rank_by_colidx is not None
        and bool(headers)
        and isinstance(headers[0], str)
        and headers[0].startswith("Observer")
    )

    sort_sentinel = object()
    sort_val = sort_sentinel
    rank = 0
    rows = []

    for ri, row in enumerate(data):
        vis_vals = [row[i] for i, _ in vis]

        if do_rank:
            sv = vis_vals[rank_by_colidx]
            if sv != sort_val:
                rank = ri + 1
                sort_val = sv

        cells = []
        for ci, (_, col) in enumerate(vis):
            raw = vis_vals[ci]
            val = h(raw)
            if ci == 0 and do_rank:
                val = f"{rank}.&nbsp;{val}"
            if ci in url_for_col:
                col_url = row[url_for_col[ci]]
                if col_url:
                    val = f'<a href="{h(col_url)}" target="_blank" rel="noopener">{val}</a>'
            if col.name == "Locality":
                cells.append(f'<td class="loc" title="{h(raw)}">{val}</td>')
            else:
                cells.append(f"<td>{val}</td>")

        cls = (
            ' class="sep"' if (hline_every and ri > 0 and ri % hline_every == 0) else ""
        )
        rows.append(f"<tr{cls}>{''.join(cells)}</tr>")

    return (
        f'<table class="dt">'
        f"<thead><tr>{th_html}</tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        f"</table>"
    )


def _tc(title, subtitle, description, column_desc, data, rank_by_colidx=-1):
    lbl = subtitle or title or ""
    out = '<div class="tc">'
    if lbl:
        out += f'<div class="tc-title">{h(lbl)}</div>'
    if description:
        out += f'<p class="desc">{h(description)}</p>'
    out += _render_table(column_desc, data, rank_by_colidx=rank_by_colidx)
    out += "</div>"
    return out


def tables_cols(table_rets, num_columns=3, rank_by_colidx=-1):
    cards = "".join(_tc(*t, rank_by_colidx=rank_by_colidx) for t in table_rets)
    return f'<div class="tgrid c{num_columns}">{cards}</div>'


def tables_in(table_rets, columns, rank_by_colidx=-1):
    nc = columns[0] if columns else 1
    parts = []
    for t in table_rets:
        title, subtitle, description, column_desc, data = t
        lbl = subtitle or title or ""
        out = '<div class="tc">'
        if lbl:
            out += f'<div class="tc-title">{h(lbl)}</div>'
        if description:
            out += f'<p class="desc">{h(description)}</p>'
        out += _render_table(column_desc, data, rank_by_colidx=rank_by_colidx)
        parts.append(out + "</div>")
    return f'<div class="tgrid c{nc}">{"".join(parts)}</div>'


def sec(anchor, title, body, description=None):
    desc = f'<p class="desc">{h(description)}</p>' if description else ""
    return f'<section class="sec" id="{h(anchor)}"><h2>{h(title)}</h2>{desc}{body}</section>\n'


def subsec(title, body, description=None):
    desc = f'<p class="desc">{h(description)}</p>' if description else ""
    return f"<div><h3>{h(title)}</h3>{desc}{body}</div>\n"


def table_sec(args, rank_by_colidx=None):
    title, subtitle, description, column_desc, data = args
    tbl = _render_table(
        column_desc,
        data,
        rank_by_colidx=rank_by_colidx if rank_by_colidx is not None else -1,
        hline_every=5,
    )
    return sec(
        anchor=title.lower().replace(" ", "-"),
        title=title,
        body=tbl,
        description=description,
    )


def _render_item_list(data, formatter):
    month_strs = {
        1: "Jan",
        2: "Feb",
        3: "Mar",
        4: "Apr",
        5: "May",
        6: "Jun",
        7: "Jul",
        8: "Aug",
        9: "Sep",
        10: "Oct",
        11: "Nov",
        12: "Dec",
    }
    items = []
    for row in data:
        if formatter == "month_big_day":
            mo = int(float(str(row[0])))
            obs, sp, dt = h(row[1]), h(row[2]), h(row[3])
            items.append(
                f'<div class="li"><strong>{month_strs[mo]}:</strong> '
                f'{sp}&nbsp;sp. &mdash; {obs} <span class="sub" style="display:inline-block;">{dt}</span></div>'
            )
        elif formatter == "day_big_day":
            mo = int(float(str(row[0])))
            day = int(float(str(row[1])))
            obs, sp = h(row[2]), h(row[3])
            yr = int(float(str(row[4])))
            items.append(
                f'<div class="li"><strong>{month_strs[mo]}-{day:02d}:</strong> '
                f'{sp}&nbsp;sp. &mdash; {obs} <span class="sub" style="display:inline-block;">({yr})</span></div>'
            )
        else:
            name = h(row[0])
            count = h(row[1]) if len(row) > 1 else ""
            sub = h(row[2]) if len(row) > 2 and row[2] is not None else None
            sub_html = f'<span class="sub">{sub}</span>' if sub else ""
            items.append(f'<div class="li">{name} ({count}){sub_html}</div>')
    return f'<div class="list-grid">{"".join(items)}</div>'


def _get_top_photos(region_code, year, limit=10):
    url = (
        f"https://media.ebird.org/api/v2/search"
        f"?regionCode={region_code}&beginYear={year}&endYear={year}"
        f"&sort=rating_rank_desc&birdOnly=true"
    )
    try:
        from fake_useragent import UserAgent

        ua = UserAgent()
        header = {"User-Agent": str(ua.chrome)}
        ss = requests.Session()
        ss.get(
            "https://media.ebird.org/catalog",
            allow_redirects=True,
            headers=header,
            timeout=10,
        )
        r = ss.get(url, headers=header, timeout=10)
        r.raise_for_status()
    except Exception as e:
        logger.warning(f"Could not fetch photos: {e}")
        return []

    results = []
    used = set()
    for pd in r.json():
        if pd["ebirdChecklistId"] in used:
            continue
        used.add(pd["ebirdChecklistId"])
        cdn = f"https://cdn.download.ams.birds.cornell.edu/api/v1/asset/{pd['assetId']}/480"
        full_cdn = f"https://cdn.download.ams.birds.cornell.edu/api/v1/asset/{pd['assetId']}/1800"
        results.append(
            {
                "url": cdn,
                "full_url": full_cdn,
                "common_name": pd["taxonomy"]["comName"],
                "user": pd["userDisplayName"],
                "rank": len(results) + 1,
            }
        )
        if len(results) >= limit:
            break
    return results


# ── Command ───────────────────────────────────────────────────────────────────


class Command(BaseCommand):
    help = "Generate an HTML Annual eBird Statistical Report"

    def add_arguments(self, parser):
        parser.add_argument("-r", "--region", default="US-DC-001")
        parser.add_argument("-y", "--year", default=2020, type=int)
        parser.add_argument(
            "--no-photos", action="store_true", help="Skip photo fetching"
        )
        parser.add_argument("-o", "--output", default=None, help="Output file path")

    def handle(self, *args, **options):
        logging.basicConfig(level="DEBUG")

        region_code = options["region"]
        year = options["year"]
        as_of = f"{year}-12-31"
        version = "v1.1"

        # Determine region where-clause and description
        parts = region_code.split("-")
        if len(parts) == 1:
            region_where_clause = f"country_code = '{region_code}'"
            rec = EBird.objects.filter(country_code=region_code).first()
            region_description = rec.country
        elif len(parts) == 2:
            region_where_clause = f"state_code = '{region_code}'"
            rec = EBird.objects.filter(state_code=region_code).first()
            region_description = f"{rec.state}, {rec.country}"
        elif len(parts) == 3:
            region_where_clause = f"county_code = '{region_code}'"
            rec = EBird.objects.filter(county_code=region_code).first()
            region_description = (
                rec.county if rec.county == rec.state else f"{rec.county}, {rec.state}"
            )
        else:
            raise RuntimeError("Unknown region code format")

        # Photos
        photos = []
        if not options["no_photos"]:
            logger.info("Fetching top photos...")
            photos = _get_top_photos(region_code, year, limit=10)

        # ── Build page ────────────────────────────────────────────────────────
        toc_items = []
        sections = []

        def add_sec(anchor, title, body, description=None):
            toc_items.append((anchor, title))
            sections.append(sec(anchor, title, body, description))

        # About
        add_sec(
            "about",
            "About this Document",
            f"<p>This is a summary report of data entered into the eBird database for the "
            f"<strong>{h(region_description)}</strong> region, intended for the amusement of area "
            f"birders. All data comes from the eBird dataset, and as such is self-reported, "
            f"sometimes does not include older records not yet entered, and is only sometimes "
            f"reviewed or approved, so any numbers or sightings may be incorrect. Birds marked by "
            f"eBird reviewers as Exotics and not approved are typically excluded. If you have ideas "
            f"for additions or have found discrepancies, please email "
            f'<a href="mailto:scott.stafford@gmail.com">scott.stafford@gmail.com</a>.</p>'
            "<p>Source: eBird Basic Dataset. Version: EBD_relDec-2025. Cornell Lab of Ornithology, Ithaca, New York. Dec 2025.</p>",
        )

        # Year in Review
        year_review_body = tables_cols(
            [
                Queries.year_stats(region_where_clause, as_of=as_of, year=year),
                Queries.new_birds(region_where_clause, as_of=as_of, year=year),
            ],
            num_columns=1,
        )
        add_sec(
            "year-in-review",
            "Year in Review",
            year_review_body,
            description=(
                f"Basic statistics from eBird for {region_description}, plus comparisons to "
                f"recent years. 'All Time' includes all eBird data, but since eBird is more "
                f"heavily used now, older data becomes increasingly sparse."
            ),
        )

        # Most Species Seen
        species_parts = []

        species_parts.append(
            tables_cols(
                [
                    Queries.top_year_lists(
                        region_where_clause, as_of=as_of, limit=20, include_change=True
                    ),
                    Queries.top_year_lists(
                        region_where_clause, as_of=as_of, limit=20, year=year
                    ),
                    Queries.top_year_lists(
                        region_where_clause,
                        as_of=as_of,
                        limit=20,
                        year=year,
                        last_x_years=5,
                    ),
                    Queries.top_year_lists(
                        region_where_clause,
                        as_of=as_of,
                        limit=20,
                        year=year,
                        birder_started_on_or_after_year=year,
                    ),
                ],
                num_columns=2,
                rank_by_colidx=1,
            )
        )

        # All-Time Bigs subsection
        bigs_body = tables_in(
            [
                Queries.top_all_time_year_lists(
                    region_where_clause, as_of=as_of, limit=15
                ),
                Queries.top_all_time_everyone_year_lists(
                    region_where_clause, as_of=as_of, limit=15
                ),
                Queries.top_all_time_month_lists(
                    region_where_clause, as_of=as_of, limit=10
                ),
                Queries.top_all_time_everyone_month_lists(
                    region_where_clause, as_of=as_of, limit=10
                ),
                Queries.top_all_time_day_lists(
                    region_where_clause, as_of=as_of, limit=15
                ),
                Queries.top_all_time_everyone_day_lists(
                    region_where_clause, as_of=as_of, limit=15
                ),
            ],
            columns=[2, 2, 2, 2, 2, 2],
        )
        species_parts.append(
            subsec(
                "Most Species Seen — All-Time Bigs",
                bigs_body,
                description=(
                    "The all-time highest Big Year, Month, and Day — the highest species count in "
                    "a single time period. Left: individual records. Right: 'team' records combining "
                    "all checklists posted in the region."
                ),
            )
        )

        # Off-time Bigs subsection
        _, _, month_desc, month_cols, month_data = Queries.every_month_is_a_big_month(
            region_where_clause, as_of=as_of
        )
        _, _, day_desc, day_cols, day_data = Queries.every_day_is_a_big_day(
            region_where_clause, as_of=as_of
        )
        offbigs_body = _render_item_list(month_data, "month_big_day") + subsec(
            "Every Day Is a Big Day",
            _render_item_list(day_data, "day_big_day"),
            description=day_desc,
        )
        species_parts.append(
            subsec(
                "Most Species Seen — Off-time Bigs",
                offbigs_body,
                description=month_desc,
            )
        )

        # Most Species on One List subsection
        one_list = Queries.most_species_on_one_list(
            region_where_clause, max_hours=3, max_miles=5, as_of=as_of, limit=20
        )
        species_parts.append(
            subsec(
                "Most Species Ever on One List",
                tables_in([one_list], columns=[1]),
                description=(
                    "Top scores go to individuals with the longest Complete Stationary or Traveling "
                    "lists meeting eBird guidelines (max 3 hours for Stationary, 5 miles for Traveling). "
                    "Observer names link to the checklist."
                ),
            )
        )

        # Four Seasons Championship
        fsc = Queries.four_seasons_champ(
            region_where_clause, as_of=as_of, year=year, limit=10
        )
        species_parts.append(
            subsec(
                fsc[0],
                tables_in([fsc], columns=[1]),
                description=fsc[2],
            )
        )

        # Top-ranked Photos
        if photos:
            photo_items = "".join(
                f'<div class="photo-item">'
                f'<img src="{h(p["url"])}" data-full="{h(p["full_url"])}" alt="{h(p["common_name"])}" loading="lazy">'
                f'<p>#{p["rank"]}: {h(p["common_name"])} &mdash; {h(p["user"])}</p>'
                f"</div>"
                for p in photos
            )
            photo_body = f'<div class="photo-grid">{photo_items}</div>'
            species_parts.append(
                subsec(
                    "Top-ranked eBird Media",
                    photo_body,
                    description=(
                        "Top photos for the region as ranked by eBird's algorithm based on user ratings."
                    ),
                )
            )

        # Most Species Photographed
        species_parts.append(
            subsec(
                "Most Species Photographed or Recorded",
                tables_cols(
                    [
                        Queries.top_year_lists(
                            region_where_clause,
                            as_of=as_of,
                            with_media=True,
                            limit=20,
                            include_change=True,
                        ),
                        Queries.top_year_lists(
                            region_where_clause,
                            as_of=as_of,
                            with_media=True,
                            limit=20,
                            year=year,
                        ),
                        Queries.most_seen_birds(
                            region_where_clause,
                            as_of=as_of,
                            limit=20,
                            sort="asc",
                            with_media=True,
                        ),
                        Queries.most_seen_birds(
                            region_where_clause,
                            as_of=as_of,
                            year=year,
                            limit=20,
                            sort="asc",
                            with_media=True,
                        ),
                    ],
                    num_columns=2,
                ),
                description=(
                    "Birders most avidly documenting sightings with photos or sound recordings, "
                    "and birds most frequently (or least frequently) documented."
                ),
            )
        )

        # DC Wards
        if region_code == "US-DC-001":
            species_parts.append(
                subsec(
                    "Top Life Lists by DC Ward",
                    tables_cols(
                        [
                            Queries.top_year_lists(
                                region_where_clause,
                                as_of=as_of,
                                limit=10,
                                block_name=block_name,
                                wkv=wkv,
                                include_change=True,
                                shorten_labels=True,
                            )
                            for block_name, wkv in sorted(dc_ward_wkv.items())
                        ],
                        num_columns=3,
                    ),
                )
            )

        add_sec(
            "most-species",
            "Most Species Seen",
            "".join(species_parts),
            description=(
                "The grand prize: most species seen (as reported to eBird). Only birds identified "
                "to species are counted — spuhs (e.g., 'gull sp.') and slashes (e.g., "
                "'Cooper's/Sharp-shinned Hawk') are excluded. Rookies are anyone who had never "
                "submitted a checklist in the region before the current year."
            ),
        )

        # Most Breeding Species Coded
        breeding_desc = (
            "Identifying breeding behaviors and coding them in eBird. "
            "'Confirmed' birds are worth 3 points, 'Probable' 2, 'Possible' 1. "
        )
        if region_code.startswith("US-DC") or region_code.startswith("US-MD"):
            breeding_desc += (
                "The 3rd MD/DC Breeding Bird Atlas runs 2020–2025. "
                "See https://ebird.org/atlasmddc/about. Data is self-reported and unvetted."
            )

        _, _, coded_people_desc, coded_people_cols, coded_people_data = (
            Queries.top_atlas_coded_people(
                region_where_clause, as_of=as_of, year=year, sort="desc"
            )
        )
        breeding_body = tables_cols(
            [
                Queries.top_atlas_year_lists(
                    region_where_clause, as_of=as_of, limit=20, year=year
                ),
                Queries.top_atlas_coded_birds(
                    region_where_clause, as_of=as_of, limit=20, year=year
                ),
            ],
            num_columns=2,
        ) + subsec(
            "Most Prone to Public Displays of Affection",
            _render_item_list(coded_people_data, "default"),
            description=coded_people_desc,
        )
        add_sec(
            "breeding",
            "Most Breeding Species Coded",
            breeding_body,
            description=breeding_desc,
        )

        # Most Efficient Birder
        add_sec(
            "efficiency",
            "Most Efficient Birder",
            tables_in(
                [
                    Queries.most_avg_species_per_hour(
                        region_where_clause, as_of=as_of, year=year, limit=15
                    )
                ],
                columns=[1],
            ),
            description=(
                "Most species per hour logged. Includes only complete stationary or traveling "
                "checklists over 5 minutes. Birders must have at least 10 checklists and 10 hours logged. "
                "(Yes, this is silly.)"
            ),
        )

        # Most Honest Birder
        add_sec(
            "honest",
            "Most Honest Birder",
            tables_in(
                [
                    Queries.most_honest_birder(
                        region_where_clause, as_of=as_of, limit=15
                    ),
                    Queries.most_honest_birder(
                        region_where_clause, as_of=as_of, year=year, limit=15
                    ),
                ],
                columns=[2, 2],
                rank_by_colidx=-2,
            ),
            description=(
                "Ranked by heavy usage of Slashes (e.g., Cooper's/Sharp-shinned Hawk) and "
                "Spuhs (e.g., gull sp.). If you never need a slash or spuh, you're lying to "
                "yourself. 'Unique' is how many different kinds you employed."
            ),
        )

        # Most Time in Field
        add_sec(
            "time-field",
            "Most Time Spent in Field",
            tables_cols(
                [
                    Queries.time_spent_in_field(
                        region_where_clause, as_of=as_of, limit=20, year=year
                    ),
                    Queries.time_spent_in_field(
                        region_where_clause,
                        as_of=as_of,
                        limit=20,
                        year=year,
                        last_x_years=5,
                    ),
                ],
                num_columns=2,
                rank_by_colidx=-2,
            ),
            description=(
                "Most time eBirded in the region. 'Days' are 24 hours long. 'Waking' is as a "
                "percentage of normal waking hours."
            ),
        )

        # Month Closeouts
        _, _, closeout_desc, closeout_cols, closeout_data = (
            Queries.top_month_closeout_birds(region_where_clause, as_of=as_of)
        )
        # rank_by_colidx must be per-table: all-time has trailing Chg column so
        # default -1 would rank by Chg instead of Species; total_month_ticks
        # has a trailing Avg column so -1 would rank by Avg instead of Ticks.
        closeout_body = (
            '<div class="tgrid c2">'
            + _tc(
                *Queries.top_month_closeouts(
                    region_where_clause, as_of=as_of, limit=20
                ),
                rank_by_colidx=1,
            )
            + _tc(
                *Queries.top_month_closeouts(
                    region_where_clause, as_of=as_of, year=year, limit=20
                ),
                rank_by_colidx=-1,
            )
            + _tc(
                *Queries.top_month_closeouts_best_years(
                    region_where_clause, as_of=as_of, limit=20
                ),
                rank_by_colidx=-1,
            )
            + _tc(
                *Queries.total_month_ticks(region_where_clause, as_of=as_of, limit=20),
                rank_by_colidx=1,
            )
            + "</div>"
        ) + subsec(
            "All Month Closeout Birds",
            _render_item_list(closeout_data, "default"),
            description=closeout_desc,
        )
        add_sec(
            "month-closeouts",
            "Month Closeouts",
            closeout_body,
            description=(
                "A Month Closeout is a bird seen in every month of the year. "
                "'Ticks' count every bird-month — seeing a Cardinal in all 12 months is 12 ticks."
            ),
        )

        # Top Month Life Lists
        add_sec(
            "month-lists",
            "Top Month Life Lists",
            tables_cols(
                [
                    Queries.top_year_lists(
                        region_where_clause,
                        as_of=as_of,
                        limit=10,
                        month=month,
                        include_change=True,
                        shorten_labels=True,
                    )
                    for month in range(1, 13)
                ],
                num_columns=3,
            ),
        )

        # Clean Sweeps
        woodpecker_desc = (
            "A Clean Sweep means seeing every species on a single checklist. "
            "Observer names link to the qualifying checklist. "
            "Species counted: Downy, Hairy, Yellow-bellied Sapsucker, Northern Flicker, "
            "Pileated, Red-bellied, and Red-headed Woodpecker."
        )
        sweep_parts = [
            subsec(
                "Picked a Peck of Woodpeckers",
                tables_in(
                    [
                        Queries.woodpecker_clean_sweep(
                            region_where_clause, as_of=as_of
                        ),
                        Queries.woodpecker_clean_sweep(
                            region_where_clause, as_of=as_of, year=year
                        ),
                    ],
                    columns=[2, 2],
                ),
                description=woodpecker_desc,
            ),
            subsec(
                "Warbler-a-palooza",
                tables_in(
                    [
                        Queries.warbler_single_list(
                            region_where_clause, as_of=as_of
                        ),
                        Queries.warbler_single_list(
                            region_where_clause, as_of=as_of, year=year
                        ),
                    ],
                    columns=[2, 2],
                ),
                description=(
                    "Most warbler species on a single checklist. "
                    "Warblers: any bird whose name ends in Warbler, Parula, Redstart, "
                    "Yellowthroat, or Waterthrush, plus Ovenbird. Date links to the checklist."
                ),
            ),
        ]
        add_sec("clean-sweeps", "Clean Sweeps", "".join(sweep_parts))

        # Bird's-eye View
        add_sec(
            "birds-eye",
            "Bird's-eye View",
            tables_cols(
                [
                    Queries.most_seen_birds(
                        region_where_clause,
                        as_of=as_of,
                        year=year,
                        limit=25,
                        sort="desc",
                    ),
                    Queries.most_seen_birds(
                        region_where_clause,
                        as_of=as_of,
                        year=year,
                        limit=25,
                        sort="asc",
                    ),
                    Queries.most_seen_birds(
                        region_where_clause,
                        as_of=as_of,
                        limit=25,
                        sort="asc",
                        year=year,
                        last_x_years=5,
                    ),
                ],
                num_columns=3,
            ),
            description=(
                "Birding is a two-way street. Sadly, birds don't eBird, so this data is "
                "necessarily incomplete. We use people lists to measure how many birders each "
                "species got to see during the year."
            ),
        )

        # Most Infrequent Visitors
        sections.append(
            table_sec(
                Queries.least_reported_birds(
                    region_where_clause, as_of=as_of, year=year, last_x_years=20
                )
            )
        )
        toc_items.append(
            (
                "most-infrequent-visitors-of-the-last-20-years",
                "Most Infrequent Visitors",
            )
        )

        # ── Assemble HTML ─────────────────────────────────────────────────────
        toc_html = "\n".join(
            f'<li><a href="#{h(anchor)}">{h(title)}</a></li>'
            for anchor, title in toc_items
        )

        cover_photo_html = ""
        if photos:
            p = photos[0]
            cover_photo_html = (
                f'<img class="cover-photo" src="{h(p["url"])}" alt="{h(p["common_name"])}">'
                f'<p class="cover-caption">Top-rated photo: {h(p["common_name"])} — ©{year} {h(p["user"])}</p>'
            )

        page_title = f"{year} Annual eBird Statistical Report — {region_description}"

        html_out = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{h(page_title)}</title>
  <style>{CSS}</style>
</head>
<body>

<header class="cover">
  <h1>Annual eBird Statistical Report</h1>
  <div class="region">{h(region_description)}</div>
  <div class="year-ver">{year} &nbsp;&bull;&nbsp; {h(version)}</div>
  {cover_photo_html}
</header>

<div class="layout">
  <nav class="toc">
    <h3>Contents</h3>
    <ol>{toc_html}</ol>
  </nav>
  <main>
    {"".join(sections)}
  </main>
</div>

<div id="lb"><img id="lb-img" src="" alt=""></div>
<script>
(function(){{
  var lb = document.getElementById('lb');
  var lbImg = document.getElementById('lb-img');
  document.querySelectorAll('.photo-item img').forEach(function(img) {{
    img.addEventListener('click', function() {{
      lbImg.src = img.dataset.full;
      lbImg.alt = img.alt;
      lb.classList.add('open');
    }});
  }});
  lb.addEventListener('click', function() {{ lb.classList.remove('open'); }});
  document.addEventListener('keydown', function(e) {{ if (e.key === 'Escape') lb.classList.remove('open'); }});
}})();
</script>
</body>
</html>
"""

        filename = (
            options["output"]
            or f"{year} Annual eBird Statistical Report - {region_code} - {region_description} - {version}.html"
        )
        with open(filename, "w", encoding="utf-8") as f:
            f.write(html_out)

        print(f"Written: {filename}")
        logger.info(f'"{filename}"')
