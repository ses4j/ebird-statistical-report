# encoding: utf-8

import calendar
import collections
import csv
import datetime
import glob
import logging
import os
import time
from datetime import date, timedelta

import numpy as np
from pylatex.base_classes.containers import Environment
from pylatex.basic import SmallText
from pylatex.package import Package
from diskcache import Cache
from django.conf import settings
from django.contrib.gis.geos import GEOSGeometry, Point, Polygon, fromstr
from django.core.management.base import BaseCommand
from django.db import connection
from django.db.models import Count, Lookup, Transform
from django.db.models.fields import Field
from pylatex import (
    Alignat,
    Axis,
    Command as LatexCommand,
    Document,
    Figure,
    Foot,
    Head,
    HFill,
    HorizontalSpace,
    LargeText,
    LineBreak,
    LongTabu,
    Math,
    Matrix,
    MediumText,
    MiniPage,
    MultiColumn,
    NewPage,
    PageStyle,
    Plot,
    Section,
    StandAloneGraphic,
    Subsection,
    Tabu,
    Tabular,
    Tabularx,
    TextColor,
    TikZ,
    VerticalSpace,
    simple_page_number,
)
from pylatex.position import Center, FlushLeft, FlushRight
from pylatex.table import Tabu
from pylatex.utils import NoEscape, bold, escape_latex, italic

# from ebirdcore.mddcbbc_block_wkv import mddcbbc_block_wkv
from ebirdcore.dc_ward_wkv import dc_ward_wkv
from ebirdcore.models import EBird
from ebirdcore.utils import get_observer_name
from ebirdcore.sql_utils import fmt, fmtrow, format_list_of_names, namedtuplefetchall

cache = Cache("cachedir")
logger = logging.getLogger(__name__)


class TitlePage(Environment):
    r"""titlepage env."""


@Field.register_lookup
class NotEqualLookup(Lookup):
    lookup_name = "ne"

    def as_sql(self, compiler, connection):
        lhs, lhs_params = self.process_lhs(compiler, connection)
        rhs, rhs_params = self.process_rhs(compiler, connection)
        params = lhs_params + rhs_params
        return "%s <> %s" % (lhs, rhs), params


from collections import namedtuple

FakeColumn = namedtuple("FakeColumn", ("name", "type"))


def comma_join(lst):
    if isinstance(lst, int):
        return str(lst)
    return ", ".join([str(_) for _ in lst])


def _get_full_where_clause(
    region_where_clause, as_of, year=None, month=None, last_x_years=None
):
    where = ""
    if year is not None:
        title = f"Year List"
        if last_x_years:
            subtitle = f"{year-last_x_years+1}-{year}"
            where += (
                f" AND extract(year from OBSERVATION_DATE) >= {year-last_x_years+1}"
            )
        else:
            subtitle = f"{year}"
            where += f" AND extract(year from OBSERVATION_DATE) = {year}"

    elif month is not None:
        where += f" AND extract(month from OBSERVATION_DATE) = {month}"
        title = f"Month Life List ({calendar.month_abbr[month]})"
        subtitle = calendar.month_abbr[month]
    else:
        title = f"Life List"
        subtitle = "All Time"

    full_where = f"""
        where {region_where_clause}
        and (category = 'species' or category = 'issf' or category = 'form' or common_name = 'Rock Pigeon')
        and observation_date <= '{as_of}'
        {where}
        """
    return full_where


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument("-r", "--region", default="US-DC-001")
        parser.add_argument("-y", "--year", default=2020, type=int)

    def handle(self, *args, **options):
        logging.basicConfig(level="DEBUG")

        region_code = options["region"]
        region_code_split = region_code.split("-")
        if len(region_code_split) == 1:
            region_where_clause = f"country_code = '{region_code}'"
            one_record = EBird.objects.filter(country_code=region_code).first()

            region_description = f"{one_record.country}"

        elif len(region_code_split) == 2:
            region_where_clause = f"state_code = '{region_code}'"
            one_record = EBird.objects.filter(state_code=region_code).first()

            region_description = f"{one_record.state}, {one_record.country}"

        elif len(region_code_split) == 3:
            region_where_clause = f"county_code = '{region_code}'"
            one_record = EBird.objects.filter(county_code=region_code).first()

            region_description = f"{one_record.county}, {one_record.state}"
            if one_record.county == one_record.state:
                region_description = f"{one_record.county}"
        else:
            raise RuntimeError("unknonw region code type")

        year = options["year"]
        as_of = f"{year}-12-31"

        geometry_options = {
            # "landscape": True,
            "paper": "letterpaper",
            "margin": "0.5in",
            "headheight": "10pt",
            "headsep": "10pt",
            "includeheadfoot": True,
        }
        doc = Document(
            page_numbers=True, geometry_options=geometry_options, indent=False
        )

        doc.packages.append(Package("multicol"))
        doc.packages.append(Package("caption"))
        doc.packages.append(Package("inputenc"))

        thanks = (
            "Data extracted from eBird Basic Dataset. Version: EBD_relDec-2021. "
            "Cornell Lab of Ornithology, Ithaca, New York. Dec 2021."
        )
        version = "v1.0"
        doc.preamble.append(
            LatexCommand(
                "title",
                NoEscape(
                    f"Annual eBird Statistical Report\\thanks{{{escape_latex(thanks)}}} \\\\ {escape_latex(region_description)} \\\\ {year} \\\\ {escape_latex(version)}"
                ),
            )
        )

        doc.preamble.append(LatexCommand("author", "Scott Stafford"))
        doc.preamble.append(LatexCommand("date", NoEscape(r"\today")))

        doc.append(NoEscape(r"\captionsetup{labelformat=empty}"))

        def get_top_ranked_photo(region_code, year):
            import requests, shutil

            r = requests.get(
                f"https://ebird.org/media/catalog.json?searchField=user&q=&regionCode={region_code}&mediaType=p&hotspot=&customRegion=&mr=M1TO12&bmo=1&emo=12&yr=YCUSTOM&by={year}&ey={year}&user=&view=Gallery&sort=rating_rank_desc&includeUnconfirmed=T&_req=&cap=no&subId=&catId=&_spec=&specId=&collectionCatalogId=&dsu=-1&action=reset_status&start=0"
            )
            photo_data = r.json()["results"]["content"][0]
            photo_url = photo_data["mediaUrl"]
            common_name = photo_data["commonName"]
            user_display_name = photo_data["userDisplayName"]
            img_response = requests.get(photo_url, stream=True)
            img_filename = f"top-image-{region_code}-{year}.jpg"
            with open(img_filename, "wb") as out_file:
                shutil.copyfileobj(img_response.raw, out_file)
            del img_response
            caption = f"{common_name} - The top-rated photo in {region_description} for {year} - ©{year} {user_display_name}"
            return {"caption": caption, "image_filename": img_filename}

        photo_data = get_top_ranked_photo(region_code=region_code, year=year)

        with doc.create(TitlePage()):
            doc.append(NoEscape(r"\maketitle"))
            doc.append(NoEscape(r"\thispagestyle{empty}"))

            with doc.create(Figure(position="h!")) as kitten_pic:
                kitten_pic.add_image(photo_data["image_filename"], width="5in")
                kitten_pic.add_caption(photo_data["caption"])

            doc.append(NewPage())

        doc.append(NoEscape(r"\tableofcontents"))

        with doc.create(Section("About this Document")):
            doc.append(
                f"This is a summary report of data entered into the eBird database for the {region_description} region, intended for the amusement of area birders. "
                "All data comes from the eBird dataset, and as such is self-reported and only sometimes reviewed or approved, so any numbers or sightings have the potential "
                "to be incorrect. "
                "If you have ideas for other lists or data that could be added "
                "or have identified data discrepancies, please email scott.stafford@gmail.com. "
            )

        with doc.create(Section("Year in Review")):
            add_section_description(
                doc,
                f"First, some basic statistics from the eBird database for {region_description}, and comparisons to recent years. "
                "'All Time' includes all data in eBird, but since eBird is much more heavily used now than before, older data becomes increasingly spotty.",
            )

            add_tables_in_columns(
                doc,
                [
                    self.year_stats(
                        region_where_clause, as_of=as_of, year=year, limit=20
                    ),
                    self.new_birds(
                        region_where_clause,
                        as_of=as_of,
                        year=year,
                    ),
                ],
                num_columns=1,
            )

        # doc.append(NewPage())

        with doc.create(Section("Most Species Seen")):
            add_section_description(
                doc,
                "Without further ado, the grand prize: most species seen (as reported to eBird)! Note that only birds identified to species are counted. "
                "Thus, a bird entered as 'Selasphorus sp.' (a 'spuh') or 'Glossy/White-faced Ibis' (a 'slash') will not be included. "
                "As elsewhere in this report, All Time is necessarily limited to data that has been entered into eBird. (If your total should be higher, go add in those old Historical lists!) "
                "Rookies are defined as anyone who has never submitted a checklist in the region before the current year.",
            )
            add_tables_in_columns(
                doc,
                [
                    self.top_year_lists(region_where_clause, as_of=as_of, limit=20),
                    self.top_year_lists(
                        region_where_clause, as_of=as_of, limit=20, year=year
                    ),
                    self.top_year_lists(
                        region_where_clause,
                        as_of=as_of,
                        limit=20,
                        year=year,
                        last_x_years=5,
                    ),
                    self.top_year_lists(
                        region_where_clause,
                        as_of=as_of,
                        limit=20,
                        year=year,
                        birder_started_on_or_after_year=year,
                    ),
                ],
                num_columns=2,
            )

        with doc.create(Section("Most Species Seen - All Time Bigs")):
            add_section_description(
                doc,
                "Here we present the all-time highest Big Year, Month, and Day -- the highest species count in a single time period. On the left are individual records. "
                "On the right are 'team' records, combining the species lists of all checklists posted in the region.",
            )

            limit = 15
            add_tables_in(
                doc,
                [
                    self.top_all_time_year_lists(
                        region_where_clause, as_of=as_of, limit=limit
                    ),
                    self.top_all_time_everyone_year_lists(
                        region_where_clause, as_of=as_of, limit=limit
                    ),
                    self.top_all_time_month_lists(
                        region_where_clause, as_of=as_of, limit=limit
                    ),
                    self.top_all_time_everyone_month_lists(
                        region_where_clause, as_of=as_of, limit=limit
                    ),
                    self.top_all_time_day_lists(
                        region_where_clause, as_of=as_of, limit=limit
                    ),
                    self.top_all_time_everyone_day_lists(
                        region_where_clause, as_of=as_of, limit=limit
                    ),
                ],
                columns=[2, 2, 2, 2, 2, 2],
            )

        with doc.create(Section("Most Species Ever on One List")):
            add_section_description(
                doc,
                "Top scores here go to individuals with the longest Complete Stationary or Traveling lists that meet eBird checklist guidelines (max 3 hours, max 5 miles, https://support.ebird.org/en/support/solutions/articles/48000795623-ebird-rules-and-best-practices).",
            )

            add_tables_in(
                doc,
                [
                    self.top_all_time_lists(
                        region_where_clause,
                        max_hours=3,
                        max_miles=5,
                        as_of=as_of,
                        limit=20,
                    ),
                ],
                columns=[1],
            )

        add_table_section(
            doc,
            self.four_seasons_champ(
                region_where_clause, as_of=as_of, limit=20, year=year
            ),
        )

        with doc.create(Section("Most Species Photographed or Recorded")):
            add_section_description(
                doc,
                "This section is dedicated to the birders most avidly documenting their sightings with photos or sound recordings, and the birds most avidly avoiding documentation.",
            )
            add_tables_in_columns(
                doc,
                [
                    self.top_year_lists(
                        region_where_clause, as_of=as_of, with_media=True, limit=20
                    ),
                    self.top_year_lists(
                        region_where_clause,
                        as_of=as_of,
                        with_media=True,
                        limit=20,
                        year=year,
                    ),
                    self.most_seen_birds(
                        region_where_clause,
                        as_of=as_of,
                        limit=20,
                        sort="asc",
                        with_media=True,
                    ),
                    self.most_seen_birds(
                        region_where_clause,
                        as_of=as_of,
                        year=year,
                        limit=20,
                        sort="asc",
                        with_media=True,
                    ),
                ],
                num_columns=2,
            )

        if region_code == "US-DC-001":
            with doc.create(Section("Top Life Lists by DC Ward")):
                # add_section_description(doc, "Top DC month listers for every month.")

                add_tables_in_columns(
                    doc,
                    [
                        self.top_year_lists(
                            region_where_clause,
                            as_of=as_of,
                            limit=10,
                            block_name=block_name,
                            wkv=wkv,
                        )
                        for block_name, wkv in sorted(dc_ward_wkv.items())
                    ],
                    num_columns=3,
                )

        with doc.create(Section("Most Breeding Species Coded")):
            desc = (
                "This section involves identifying breeding behaviors and coding them in eBird ('coding' means assigning a Breeding Code). "
                "The Score was described in an interview with Alex Wiebe at https://ebird.org/news/breedingbird2016/; "
                "'Confirmed' breeding birds are worth 3 points, 'Probable' breeding codes are worth 2, and 'Possible' codes are worth 1. "
            )

            if region_code.startswith("US-DC") or region_code.startswith("US-MD"):
                desc += (
                    "This is particularly useful right now as the 3rd MD/DC Breeding Bird Atlas just completed the first year of a 5 year run. "
                    "If you're not aware of the Atlasing effort, please see https://ebird.org/atlasmddc/about. "
                    "Includes lists not specifically in an Atlas portal, and as elsewhere in this report the data is self-reported and unvetted, so it may differ from final Atlas figures. "
                )

            add_section_description(doc, desc)
            add_tables_in_columns(
                doc,
                [
                    self.top_atlas_year_lists(
                        region_where_clause,
                        as_of=as_of,
                        limit=20,
                        year=year,
                    ),
                    self.top_atlas_coded_birds(
                        region_where_clause,
                        as_of=as_of,
                        limit=20,
                        year=year,
                    ),
                ],
                num_columns=2,
            )
            add_list_section(
                doc,
                self.top_atlas_coded_people(
                    region_where_clause, as_of=as_of, year=year, sort="desc"
                ),
            )

        with doc.create(Section("Most Efficient Birder")):
            add_section_description(
                doc,
                "A list of the most efficient birders, in terms of seeing the most species per hour logged. "
                "Includes only complete stationary or traveling checklists over 5 minutes in duration. "
                "Birders also must have at least 10 checklists and at least 10 hours logged. "
                "(PS Yes, I know this is silly.)",
            )
            add_tables_in(
                doc,
                [
                    self.most_avg_species_per_hour(
                        region_where_clause, as_of=as_of, year=year, limit=15
                    ),
                ],
                columns=[1],
            )

        with doc.create(Section("Most Honest Birder")):
            add_section_description(
                doc,
                "A list of the most honest birders, as measured by heavy usage of Slashes (eg Cooper's/Sharp-shinned Hawk) and Spuhs (eg gull sp.). If you never need a slash or a spuh, you're lying either to us or to yourself. "
                "(PS Yes, I know this is possibly sillier than the last one.)",
            )
            add_tables_in(
                doc,
                [
                    self.most_honest_birder(region_where_clause, as_of=as_of, limit=15),
                    self.most_honest_birder(
                        region_where_clause, as_of=as_of, year=year, limit=15
                    ),
                ],
                columns=[2, 2],
                rank_by_colidx=-2,
            )

        with doc.create(Section("Most Time Spent in Field")):
            add_section_description(
                doc,
                "Rankings of most time eBirded in the region.  'Days' are 24 hours long. 'Waking' is as a percentage of normal waking hours. (And this doesn't even include driving to locations, adding media to checklists, etc!)",
            )
            add_tables_in_columns(
                doc,
                [
                    self.time_spent_in_field(
                        region_where_clause, as_of=as_of, limit=20, year=year
                    ),
                    self.time_spent_in_field(
                        region_where_clause,
                        as_of=as_of,
                        limit=20,
                        year=year,
                        last_x_years=5,
                    ),
                ],
                num_columns=2,
                rank_by_colidx=-2,
            )

        with doc.create(Section("Month Closeouts")):
            add_section_description(
                doc, "A Month Closeout is a bird seen in every month of the year."
            )
            add_tables_in_columns(
                doc,
                [
                    self.top_month_closeouts(
                        region_where_clause, as_of=as_of, limit=20
                    ),
                    self.top_month_closeouts(
                        region_where_clause, as_of=as_of, year=year, limit=20
                    ),
                    self.top_month_closeouts_best_years(
                        region_where_clause, as_of=as_of, limit=20
                    ),
                    self.total_month_ticks(region_where_clause, as_of=as_of, limit=20),
                ],
                num_columns=2,
            )
            add_list_section(
                doc, self.top_month_closeout_birds(region_where_clause, as_of=as_of)
            )

        with doc.create(Section("Top Month Life Lists")):
            add_section_description(doc, "Top month listers for each month, all time.")

            add_tables_in_columns(
                doc,
                [
                    self.top_year_lists(
                        region_where_clause, as_of=as_of, limit=10, month=month
                    )
                    for month in range(1, 13)
                ],
                num_columns=3,
            )

        # with doc.create(Section("2020 birds")):
        #     add_section_description(
        #         doc,
        #         "hi"
        #         # "Birding is a two-way street. Sadly, the birds themselves don't eBird so this data is necessarily incomplete. "
        #         # "As a surrogate, we use people lists to identify how many birders each species got to see during the year.",
        #     )

        with doc.create(Section("Bird's-eye View")):
            add_section_description(
                doc,
                "Birding is a two-way street. Sadly, the birds themselves don't eBird so this data is necessarily incomplete. "
                "As a surrogate, we use people lists to identify how many birders each species got to see during the year.",
            )

            limit = 25
            add_tables_in_columns(
                doc,
                [
                    self.most_seen_birds(
                        region_where_clause,
                        as_of=as_of,
                        year=year,
                        limit=limit,
                        sort="desc",
                    ),
                    self.most_seen_birds(
                        region_where_clause,
                        as_of=as_of,
                        year=year,
                        limit=limit,
                        sort="asc",
                    ),
                    self.most_seen_birds(
                        region_where_clause,
                        as_of=as_of,
                        limit=limit,
                        sort="asc",
                        year=2020,
                        last_x_years=5,
                    ),
                ],
                num_columns=3,
            )

        add_table_section(
            doc,
            self.least_reported_birds(
                region_where_clause, as_of=as_of, year=year, last_x_years=20
            ),
        )

        print("generating pdf...")
        filename_base = f"{year} Annual eBird Statistical Report - {region_code}"
        if version:
            filename_base += " - " + version

        doc.generate_pdf(filename_base, clean_tex=False)

        logger.info(f'"{filename_base}.pdf"')

    @staticmethod
    def year_stats(
        region_where_clause, as_of, limit=10, year=None, month=None, last_x_years=None
    ):
        title = ""
        subtitle = title
        logger.debug(f"Generating {title}...")

        cols = [
            FakeColumn("", 25),
            FakeColumn(str(year - 4), 20),
            FakeColumn(str(year - 3), 20),
            FakeColumn(str(year - 2), 20),
            FakeColumn(str(year - 1), 20),
            FakeColumn(str(year), 20),
            FakeColumn("All Time", 20),
        ]

        data = []

        def add_to_table(title, where_clauses, sql_template):
            vals = []
            for where in where_clauses:
                logger.debug(f"Generating {title} ({where_clauses[0]})...")
                sql = sql_template.format(where=where)
                cols, val = execute_query(sql)
                vals.append(val[0][0])
            data.append([title] + vals)

        where_clauses = [
            _get_full_where_clause(
                region_where_clause, as_of, year - 4, month, last_x_years
            ),
            _get_full_where_clause(
                region_where_clause, as_of, year - 3, month, last_x_years
            ),
            _get_full_where_clause(
                region_where_clause, as_of, year - 2, month, last_x_years
            ),
            _get_full_where_clause(
                region_where_clause, as_of, year - 1, month, last_x_years
            ),
            _get_full_where_clause(
                region_where_clause, as_of, year, month, last_x_years
            ),
            _get_full_where_clause(
                region_where_clause, as_of, None, month, last_x_years
            ),
        ]
        add_to_table(
            "Birders",
            where_clauses,
            """select count(distinct observer_id) from ebird {where}""",
        )
        add_to_table(
            "Species",
            where_clauses,
            """select count(distinct common_name) from ebird {where}""",
        )
        add_to_table(
            "Lists",
            where_clauses,
            """select count(distinct SAMPLING_EVENT_IDENTIFIER) from ebird {where}""",
        )
        add_to_table(
            "Time Logged in Field (in Days)",
            where_clauses,
            # """select round(sum(duration_minutes)/60.0/24.0, 1) from ebird {where}""",
            """select round(sum(cnt),1) from (select max(duration_minutes)/60.0/24.0 as cnt from ebird {where} group by SAMPLING_EVENT_IDENTIFIER) t""",
        )
        add_to_table(
            "Individual Birds",
            where_clauses,
            """select sum(cnt) from (select max(observation_count) as cnt from ebird {where} group by SAMPLING_EVENT_IDENTIFIER) t""",
        )

        # add_to_table(
        #     "Individual Birds",
        #     f"""select sum(cnt) as "Individuals" from (select max(observation_count) as cnt from ebird {_get_full_where_clause(region_where_clause, as_of, year-1, month, last_x_years)} group by SAMPLING_EVENT_IDENTIFIER) t""",
        #     f"""select sum(cnt) as "Individuals" from (select max(observation_count) as cnt from ebird {_get_full_where_clause(region_where_clause, as_of, year, month, last_x_years)} group by SAMPLING_EVENT_IDENTIFIER) t""",
        #     f"""select sum(cnt) as "Individuals" from (select max(observation_count) as cnt from ebird {_get_full_where_clause(region_where_clause, as_of, None, month, last_x_years)} group by SAMPLING_EVENT_IDENTIFIER) t""",
        # )  # cols, data = execute_query(sql)
        # breakpoint()

        return title, subtitle, None, cols, data

    @staticmethod
    def top_year_lists(
        region_where_clause,
        as_of,
        limit=10,
        year=None,
        month=None,
        with_media=False,
        last_x_years=None,
        birder_started_on_or_after_year=None,
        block_name=None,
        wkv=None,
        sort="desc",
    ):
        where = ""
        if year is not None:
            title = f"Year List"
            if last_x_years:
                subtitle = f"{year-last_x_years+1}-{year}"
                where += (
                    f" AND extract(year from OBSERVATION_DATE) >= {year-last_x_years+1}"
                )
            else:
                subtitle = f"{year}"
                where += f" AND extract(year from OBSERVATION_DATE) = {year}"

        elif month is not None:
            where += f" AND extract(month from OBSERVATION_DATE) = {month}"
            title = f"Month Life List ({calendar.month_abbr[month]})"
            subtitle = calendar.month_abbr[month]
        else:
            title = f"Life List"
            subtitle = "All Time"

        if with_media:
            where += f" AND has_media = 't'"
            title += " w/ Photo/Audio"
            # subtitle = " w/ Photo/Audio"

        if birder_started_on_or_after_year:
            where += f" AND observer_id not in (select distinct observer_id from ebird where {region_where_clause} and OBSERVATION_DATE < '{birder_started_on_or_after_year}-01-01')"
            subtitle += " (Rookies)"

        if block_name and wkv:
            block_geog = GEOSGeometry(wkv)
            where += f" and ST_Intersects(geog, ST_GeomFromEWKT('{block_geog.ewkt}'))"
            subtitle = f" {block_name}"
            title += f" {block_name}"

        sql = f"""
select get_observer_name(t.observer_id) as "Observer", count(t.common_name) as "Species"
from (
         select distinct OBSERVER_ID, COMMON_NAME
         from ebird
         where {region_where_clause}
           and (category = 'species' or category = 'issf' or category = 'form' or common_name = 'Rock Pigeon')
           and observation_date <= '{as_of}'
           {where}
     ) t
group by observer_id
order by 2 {sort}, 1 asc
limit {limit};
"""
        logger.debug(f"Generating {title}...")
        a, b = execute_query(sql)
        return title, subtitle, None, a, b

    @staticmethod
    def most_seen_birds(
        region_where_clause,
        as_of,
        year=None,
        month=None,
        last_x_years=None,
        sort="desc",
        with_media=False,
        limit=10,
    ):
        verb = "Seen" if not with_media else "Documented"
        title = f"Most {verb}"
        if sort == "desc":
            subtitle = f"Most People {verb}"
        elif sort == "asc":
            subtitle = f"Fewest People {verb}"
        else:
            raise RuntimeError()

        where = ""
        if year is not None:
            title = f"Year List"
            if last_x_years:
                subtitle += f" {year-last_x_years+1}-{year}"
                where += (
                    f" AND extract(year from OBSERVATION_DATE) >= {year-last_x_years+1}"
                )
            else:
                subtitle += f" {year}"
                where += f" AND extract(year from OBSERVATION_DATE) = {year}"

        elif month is not None:
            where += f" AND extract(month from OBSERVATION_DATE) = {month}"
            title = f"Month Life List ({calendar.month_abbr[month]})"
            subtitle += " " + calendar.month_abbr[month]
        else:
            title = f"Life List"
            subtitle += " All Time"

        if with_media:
            where += f" AND has_media = 't'"
            title += " w/ Photo/Audio"
            # subtitle = " w/ Photo/Audio"

        sql = f"""
select t.common_name as "Species", count(t.OBSERVER_ID) as "Birders"
from (
         select distinct OBSERVER_ID, COMMON_NAME
         from ebird
         where {region_where_clause}
           and (category = 'species' or category = 'issf' or category = 'form' or common_name = 'Rock Pigeon')
           --and approved = 't'
        -- and OBSERVATION_DATE >= '{year}-01-01'
        and observation_date <= '{as_of}'
        {where}
     ) t
group by t.common_name
order by 2 {sort}, 1 asc
limit {limit};
"""
        logger.debug(f"Generating {title}...")
        a, b = execute_query(sql)
        return title, subtitle, None, a, b

    @staticmethod
    def least_reported_birds(
        region_where_clause,
        as_of,
        year=None,
        last_x_years=15,
        max_years_reported=6,
    ):
        title = f"Most Infrequent Visitors of the Last {last_x_years} Years"
        subtitle = title

        sql = f"""
select common_name                                         as "Species",
       count(distinct extract(year from OBSERVATION_DATE)) as "Years Reported",
       count(distinct observer_id)                         as "Birders",
       max(OBSERVATION_DATE)                               as "Last Seen"
from ebird
where {region_where_clause}
  and (category = 'species' or category = 'issf' or category = 'form' or common_name = 'Rock Pigeon')
  and observation_date <= '{as_of}'
  and observation_date >= '{year-last_x_years+1}-01-01'
group by common_name
having count(distinct extract(year from OBSERVATION_DATE)) <= {max_years_reported}
order by 2 asc, 3 asc, 4 asc;
"""
        logger.debug(f"Generating {title}...")
        a, b = execute_query(sql)
        return title, subtitle, None, a, b

    @staticmethod
    def top_atlas_year_lists(
        region_where_clause,
        as_of,
        limit=10,
        year=None,
        month=None,
        last_x_years=None,
        birder_started_on_or_after_year=None,
        block_name=None,
        wkv=None,
    ):
        where = ""
        if year is not None:
            title = f"Year List"
            if last_x_years:
                subtitle = f"{year-last_x_years+1}-{year}"
                where += (
                    f" AND extract(year from OBSERVATION_DATE) >= {year-last_x_years+1}"
                )
            else:
                subtitle = f"{year}"
                where += f" AND extract(year from OBSERVATION_DATE) = {year}"

        elif month is not None:
            where += f" AND extract(month from OBSERVATION_DATE) = {month}"
            title = f"Month Life List ({calendar.month_abbr[month]})"
            subtitle = calendar.month_abbr[month]
        else:
            title = f"Life List"
            subtitle = "All Time"

        if birder_started_on_or_after_year:
            where += f" AND observer_id not in (select distinct observer_id from ebird where {region_where_clause} and OBSERVATION_DATE < '{birder_started_on_or_after_year}-01-01')"
            subtitle += " (Rookies)"

        if block_name and wkv:
            block_geog = GEOSGeometry(wkv)
            where += f" and ST_Intersects(geog, ST_GeomFromEWKT('{block_geog.ewkt}'))"
            subtitle = f" {block_name}"
            title += f" {block_name}"

        subtitle += " Top Breeding Challenge Score"
        sql = f"""
select get_observer_name(t.observer_id)                                                          as "Observer",
       sum(case when cat = 'C2' then 1 else 0 end)                                               as "Poss",
       sum(case when cat = 'C3' then 1 else 0 end)                                               as "Prob",
       sum(case when cat = 'C4' then 1 else 0 end)                                               as "Conf",
    --    count(t.common_name)                                                                      as "Total",
       sum(case when cat = 'C4' then 3 when cat = 'C3' then 2 when cat = 'C2' then 1 else 0 end) as "Score"
from (
         select OBSERVER_ID, COMMON_NAME, max(BREEDING_CATEGORY) as cat
         from ebird
         where {region_where_clause}
           and (category = 'species' or category = 'issf' or category = 'form' or common_name = 'Rock Pigeon')
           and observation_date <= '{as_of}'
           AND breeding_category is not null
           AND breeding_category in ('C2', 'C3', 'C4')
           {where}
         group by OBSERVER_ID, COMMON_NAME
     ) t
group by observer_id
order by "Score" desc
limit {limit};
"""
        logger.debug(f"Generating {title}...")
        a, b = execute_query(sql)
        return title, subtitle, None, a, b

    @staticmethod
    def top_atlas_coded_birds(
        region_where_clause,
        as_of,
        limit=10,
        year=None,
        last_x_years=None,
        birder_started_on_or_after_year=None,
        block_name=None,
        wkv=None,
    ):
        where = ""
        if year is not None:
            title = f"Year List"
            if last_x_years:
                subtitle = f"{year-last_x_years+1}-{year}"
                where += (
                    f" AND extract(year from OBSERVATION_DATE) >= {year-last_x_years+1}"
                )
            else:
                subtitle = f"{year}"
                where += f" AND extract(year from OBSERVATION_DATE) = {year}"

        else:
            title = f"Life List"
            subtitle = "All Time"

        if birder_started_on_or_after_year:
            where += f" AND observer_id not in (select distinct observer_id from ebird where {region_where_clause} and OBSERVATION_DATE < '{birder_started_on_or_after_year}-01-01')"
            subtitle += " (Rookies)"

        if block_name and wkv:
            block_geog = GEOSGeometry(wkv)
            where += f" and ST_Intersects(geog, ST_GeomFromEWKT('{block_geog.ewkt}'))"
            subtitle = f" {block_name}"
            title += f" {block_name}"

        subtitle += " Most Coded Birds"

        sql = f"""
select get_observer_name(t.observer_id) as "Observer",
       count(*)                         as "Coded Lists",
       sum(birds)                       as "Coded Birds"
from (
         select OBSERVER_ID, SAMPLING_EVENT_IDENTIFIER, count(*) as birds
         from ebird
         where {region_where_clause}
           and (category = 'species' or category = 'issf' or category = 'form' or common_name = 'Rock Pigeon')
           and observation_date <= '{as_of}'
           AND breeding_category is not null
           AND breeding_category in ('C2', 'C3', 'C4')
           -- and PROJECT_CODE = 'EBIRD_ATL_MD_DC'
           {where}
         group by OBSERVER_ID, SAMPLING_EVENT_IDENTIFIER
     ) t
group by observer_id
order by 3 desc
limit {limit};
"""

        logger.debug(f"Generating {title}...")
        a, b = execute_query(sql)
        return title, subtitle, None, a, b

    @staticmethod
    def top_atlas_coded_people(
        region_where_clause,
        as_of,
        limit=10,
        year=None,
        last_x_years=None,
        birder_started_on_or_after_year=None,
        block_name=None,
        wkv=None,
        num_to_credit=2,
        sort="asc",
    ):
        where = ""
        if year is not None:
            title = f"Year List"
            if last_x_years:
                subtitle = f"{year-last_x_years+1}-{year}"
                where += (
                    f" AND extract(year from OBSERVATION_DATE) >= {year-last_x_years+1}"
                )
            else:
                subtitle = f"{year}"
                where += f" AND extract(year from OBSERVATION_DATE) = {year}"

        else:
            title = f"Life List"
            subtitle = "All Time"

        if birder_started_on_or_after_year:
            where += f" AND observer_id not in (select distinct observer_id from ebird where {region_where_clause} and OBSERVATION_DATE < '{birder_started_on_or_after_year}-01-01')"
            subtitle += " (Rookies)"

        if block_name and wkv:
            block_geog = GEOSGeometry(wkv)
            where += f" and ST_Intersects(geog, ST_GeomFromEWKT('{block_geog.ewkt}'))"
            subtitle = f" {block_name}"
            title += f" {block_name}"

        # subtitle += " "
        title = "Most Prone to Public Displays of Affection"
        description = (
            "This lists every bird coded as Probable or Confirmed during the year, along with the number of people who coded it.  "
            "Includes lists not specifically in the Atlas portal, and as elsewhere in this report the data is self-reported and unvetted, so it may differ from final Atlas figures. "
            f"If {num_to_credit} or fewer birders coded it, their names are listed."
        )

        sql = f"""
select common_name as "Species",
       count(*)          as "#",
       case
           when count(*) <= {num_to_credit} then
               string_agg(get_observer_name(OBSERVER_ID), ', ' order by get_observer_name(OBSERVER_ID))
           else null end as "Birders"
from (
         select 
            common_name, 
            observer_id,
            sum(case when breeding_category = 'C4' then 1 else 0 end), 
            sum(case when breeding_category = 'C3' then 1 else 0 end) 
         from ebird
         where {region_where_clause}
           and (category = 'species' or category = 'issf' or category = 'form' or common_name = 'Rock Pigeon')
           and observation_date <= '{as_of}'
           AND breeding_category is not null
           AND breeding_category in ('C3', 'C4')
           -- and PROJECT_CODE = 'EBIRD_ATL_MD_DC'
           {where}
         group by common_name, observer_id
     ) t
group by common_name
order by "#" {sort}, common_name asc;
"""

        logger.debug(f"Generating {title}...")
        a, b = execute_query(sql)
        return title, subtitle, description, a, b

    @staticmethod
    def top_all_time_everyone_year_lists(
        region_where_clause,
        as_of,
        limit=10,
        with_media=False,
    ):
        where = ""
        subtitle = "Biggest Big Year (Everyone)"
        title = subtitle

        if with_media:
            where += f" AND has_media = 't'"
            title += " w/ Photo/Audio"
            # subtitle = " w/ Photo/Audio"

        sql = f"""
select year as "Year", count(distinct t.observer_id) as "Birders", count(distinct t.common_name) as "Species"
from (
         select extract(year from OBSERVATION_DATE)::int as year, OBSERVER_ID, COMMON_NAME
         from ebird
         where {region_where_clause}
           and (category = 'species' or category = 'issf' or category = 'form' or common_name = 'Rock Pigeon')
           and observation_date <= '{as_of}'
           {where}
     ) t
group by year
order by 3 desc
limit {limit};
"""
        logger.debug(f"Generating {title}...")
        a, b = execute_query(sql)
        return title, subtitle, None, a, b

    @staticmethod
    def top_all_time_everyone_month_lists(
        region_where_clause,
        as_of,
        limit=10,
        with_media=False,
    ):
        where = ""
        subtitle = "Biggest Big Month (Everyone)"
        title = subtitle

        if with_media:
            where += f" AND has_media = 't'"
            title += " w/ Photo/Audio"
            # subtitle = " w/ Photo/Audio"

        sql = f"""
select month as "Month", count(distinct t.observer_id) as "Birders", count(distinct t.common_name) as "Species"
from (
         select OBSERVER_ID, to_char(OBSERVATION_DATE, 'yyyy-mm') as Month, COMMON_NAME         
         from ebird
         where {region_where_clause}
           and (category = 'species' or category = 'issf' or category = 'form' or common_name = 'Rock Pigeon')
           and observation_date <= '{as_of}'
           {where}
     ) t
group by month
order by 3 desc
limit {limit};
"""
        logger.debug(f"Generating {title}...")
        a, b = execute_query(sql)
        return title, subtitle, None, a, b

    @staticmethod
    def top_month_closeouts(region_where_clause, as_of, year=None, limit=10):
        subtitle = "All-Time"
        title = f"Month Closeouts"

        where = ""
        if year is not None:
            where += f" AND extract(year from OBSERVATION_DATE) = {year}"
            subtitle = f"{year}"
        else:
            subtitle = "All-Time"
        title += " - " + subtitle

        sql = f"""
-- month closeouts
select get_observer_name(OBSERVER_ID) as "Observer",
       count(*)                                           as "Species"
       --, string_agg(COMMON_NAME, ', ' order by COMMON_NAME) as birds

from (
         select OBSERVER_ID, COMMON_NAME, count(*) as num_months
         from (
                  select to_char(OBSERVATION_DATE, 'Mon') as mon, OBSERVER_ID, COMMON_NAME
                  from ebird
                  where {region_where_clause}
                    and (category = 'species' or category = 'issf' or category = 'form' or common_name = 'Rock Pigeon')
                    and observation_date <= '{as_of}'
                    {where}
                  group by 1, 2, 3) t
         group by OBSERVER_ID, COMMON_NAME
         order by num_months desc) u
where num_months = 12
group by OBSERVER_ID, num_months
order by "Species" desc
limit {limit};
"""
        logger.debug(f"Generating {title}...")
        a, b = execute_query(sql)
        return title, subtitle, None, a, b

    @staticmethod
    def top_month_closeout_birds(
        region_where_clause, as_of, sort="desc", num_to_credit=2
    ):
        subtitle = "All Month Closeout Birds"
        title = subtitle
        description = f"This is a list of all birds in the region that have been seen in every month of the year (in any year).  If {num_to_credit} or fewer birders have closed it out, their names are listed."

        sql = f"""
select common_name as "Species",
       count(*)          as "#",
       case
           when count(*) <= {num_to_credit} then
               string_agg(get_observer_name(OBSERVER_ID), ', ' order by get_observer_name(OBSERVER_ID))
           else null end as "Birders"
from (
         select OBSERVER_ID, COMMON_NAME, count(*) as num_months
         from (
                  select to_char(OBSERVATION_DATE, 'Mon') as mon, OBSERVER_ID, COMMON_NAME
                  from ebird
                  where {region_where_clause}
                    and (category = 'species' or category = 'issf' or category = 'form' or common_name = 'Rock Pigeon')
                    and observation_date <= '{as_of}'
                  group by 1, 2, 3) t
         group by OBSERVER_ID, COMMON_NAME
         order by num_months desc) u
where num_months = 12
group by common_name, num_months
order by "#" {sort};
"""

        logger.debug(f"Generating {title}...")
        a, b = execute_query(sql)
        return title, subtitle, description, a, b

    @staticmethod
    def total_month_ticks(region_where_clause, as_of, limit=10):
        subtitle = "All-Time Month Ticks"
        title = f"Total Month Ticks"
        description = ""

        sql = f"""
select get_observer_name(OBSERVER_ID) as "Observer",
       count(*)                       as "Ticks",
       round(count(*)/12.0,1)                       as "Avg Per Mo."

from (
         select distinct to_char(OBSERVATION_DATE, 'Mon') as mon, OBSERVER_ID, COMMON_NAME
         from ebird
         where {region_where_clause}
           and (category = 'species' or category = 'issf' or category = 'form' or common_name = 'Rock Pigeon')
           and observation_date <= '{as_of}'
           ) t

group by OBSERVER_ID
order by "Ticks" desc
limit {limit};
"""

        logger.debug(f"Generating {title}...")
        a, b = execute_query(sql)
        return title, subtitle, description, a, b

    @staticmethod
    def time_spent_in_field(
        region_where_clause, as_of, year, last_x_years=None, limit=10
    ):
        description = "Sum of time listed on Stationary or Traveling counts with duration included. Excludes any lists over 10 hours."
        where = ""
        subtitle = None
        if last_x_years is None:
            assert year
            where += f" AND extract(year from OBSERVATION_DATE) = {year}"
            title = f"Most Time Spent In Field ({year})"
            subtitle = f"{year}"
            waking_hours = 5840

        else:
            where += (
                f" AND extract(year from OBSERVATION_DATE) >= {year-last_x_years+1}"
            )
            # where += f" AND extract(year from OBSERVATION_DATE) < {year-5}"
            title = f"Most Time Spent In Field (last {last_x_years} years)"
            # subtitle = f"last {last_x_years} years"
            subtitle = f"{year-last_x_years+1}-{year}"
            waking_hours = 5840 * last_x_years

        sql = f"""
select get_observer_name(t.observer_id) as "Observer", 
    count(*) as "Lists", 
    round(sum(duration_minutes) / 60 / 24.0, 2) as "Days", 
    round(100.0 * sum(duration_minutes) / 60 / {waking_hours}, 1)::text || '%' as "Waking"
from (select observer_id, min(duration_minutes) duration_minutes
      from ebird
      where true
        and {region_where_clause}
        and (category = 'species' or category = 'issf' or category = 'form' or common_name = 'Rock Pigeon')
        and observation_date <= '{as_of}'
        and duration_minutes is not null
        and duration_minutes <= 600
        and protocol_code in ('P21', 'P22')
        {where}
      group by observer_id, SAMPLING_EVENT_IDENTIFIER
     ) t
group by observer_id
order by 3 desc
limit {limit};
"""
        logger.debug(f"Generating {title}...")
        a, b = execute_query(sql)
        return title, subtitle, description, a, b

    @staticmethod
    def top_month_closeouts_best_years(region_where_clause, as_of, limit=10):
        where = ""
        subtitle = "Best Years"
        title = f"Month Closeouts -- Best Years"

        sql = f"""
-- all-time best month closeouts
select get_observer_name(OBSERVER_ID) as "Observer",
       Year,
       count(*)                                           as "Species"

from (
         select OBSERVER_ID, year, COMMON_NAME, count(*) as num_months
         from (
                  select to_char(OBSERVATION_DATE, 'Mon') as mon, extract(year from OBSERVATION_DATE)::int as Year, OBSERVER_ID, COMMON_NAME
                  from ebird
                  where {region_where_clause}
                    and (category = 'species' or category = 'issf' or category = 'form' or common_name = 'Rock Pigeon')
                    and observation_date <= '{as_of}'
                  group by 1, 2, 3, 4) t
         group by OBSERVER_ID, year, COMMON_NAME
         order by num_months desc) u
where num_months = 12
group by OBSERVER_ID, year, num_months
order by "Species" desc
limit {limit};
"""

        logger.debug(f"Generating {title}...")
        a, b = execute_query(sql)
        return title, subtitle, None, a, b

    @staticmethod
    def top_all_time_year_lists(region_where_clause, as_of, limit=10):
        where = ""
        title = f"All-Time Top Year List"
        subtitle = "Biggest Big Year"

        sql = f"""
select get_observer_name(t.observer_id) as "Observer", Year as "Year", count(t.common_name) as "Species"
from (
         select distinct OBSERVER_ID, extract(year from OBSERVATION_DATE)::int as Year, COMMON_NAME
         from ebird
         where {region_where_clause}
           and (category = 'species' or category = 'issf' or category = 'form' or common_name = 'Rock Pigeon')
           and observation_date <= '{as_of}'
     ) t
group by observer_id, year
order by 3 desc, 2 asc, 1 asc
limit {limit};
"""

        logger.debug(f"Generating {title}...")
        a, b = execute_query(sql)
        return title, subtitle, None, a, b

    @staticmethod
    def four_seasons_champ(region_where_clause, as_of, year, limit=10):
        where = ""
        title = f"Four Seasons Championship"
        subtitle = title
        description = (
            "The Four Seasons Championship is a competition idea I've toyed with. Most Big Days are during migration, but they don't have to be! "
            "The idea is to schedule a Big Day in the peak of each of the four seasons. Sum up the tally from each of the four days, and the person with the best score is the Champ. "
            "Since this hasn't actually been organized, for now this ranking will suffice: The sum of each person's best day in each of the four seasons (Mar-May, Jun-Jul, Aug-Nov, Dec-Feb)."
        )

        sql = f"""
with birdies as (
    select observer_id, OBSERVATION_DATE, count(distinct common_name) as species
    from ebird
    where true
      and {region_where_clause}
      and (category = 'species' or category = 'issf' or category = 'form' or common_name = 'Rock Pigeon')
      and OBSERVATION_DATE BETWEEN '2020-01-01' and '2020-12-31'
      and observation_date <= '{as_of}'
    group by observer_id, OBSERVATION_DATE
)
select get_observer_name(b2.observer_id) as "Observer",
       (select coalesce(max(species), 0)
        from birdies b
        where b.observer_id = b2.observer_id
          and OBSERVATION_DATE BETWEEN '{year}-03-01' and '{year}-05-31')                "Spring",
       (select coalesce(max(species), 0)
        from birdies b
        where b.observer_id = b2.observer_id
          and OBSERVATION_DATE BETWEEN '{year}-06-01' and '{year}-07-31')                "Summer",
       (select coalesce(max(species), 0)
        from birdies b
        where b.observer_id = b2.observer_id
          and OBSERVATION_DATE BETWEEN '{year}-08-01' and '{year}-11-30')                "Fall",
       (select coalesce(max(species), 0)
        from birdies b
        where b.observer_id = b2.observer_id
          and (OBSERVATION_DATE < '{year}-03-01' or OBSERVATION_DATE >= '{year}-12-01')) "Winter",

       (select coalesce(max(species), 0)
        from birdies b
        where b.observer_id = b2.observer_id
          and OBSERVATION_DATE BETWEEN '{year}-03-01' and '{year}-05-31') +
       (select coalesce(max(species), 0)
        from birdies b
        where b.observer_id = b2.observer_id
          and OBSERVATION_DATE BETWEEN '{year}-06-01' and '{year}-07-31') +
       (select coalesce(max(species), 0)
        from birdies b
        where b.observer_id = b2.observer_id
          and OBSERVATION_DATE BETWEEN '{year}-08-01' and '{year}-11-30') +
       (select coalesce(max(species), 0)
        from birdies b
        where b.observer_id = b2.observer_id
          and (OBSERVATION_DATE < '{year}-03-01' or OBSERVATION_DATE >= '{year}-12-01')) "Score"

from birdies b2
group by observer_id
order by "Score" desc
limit {limit};
"""

        logger.debug(f"Generating {title}...")
        a, b = execute_query(sql)
        return title, subtitle, description, a, b

    @staticmethod
    def top_all_time_month_lists(region_where_clause, as_of, limit=10):
        where = ""
        title = f"All-Time Top Month List"
        subtitle = "Biggest Big Month"

        sql = f"""
select get_observer_name(t.observer_id) as "Observer", month as "Month", count(t.common_name) as "Species"
from (
         select distinct OBSERVER_ID, to_char(OBSERVATION_DATE, 'yyyy-mm') as Month, COMMON_NAME
         from ebird
         where {region_where_clause}
           and (category = 'species' or category = 'issf' or category = 'form' or common_name = 'Rock Pigeon')
           and observation_date <= '{as_of}'
     ) t
group by observer_id, Month
order by 3 desc, 2 asc, 1 asc
limit {limit};
"""

        logger.debug(f"Generating {title}...")
        a, b = execute_query(sql)
        return title, subtitle, None, a, b

    @staticmethod
    def top_all_time_day_lists(region_where_clause, as_of, limit=10):
        where = ""
        title = f"All-Time Top Day List"
        subtitle = "Biggest Big Day"

        sql = f"""
select get_observer_name(t.observer_id) as "Observer", OBSERVATION_DATE as "Date", count(t.common_name) as "Species"
from (
         select distinct OBSERVER_ID, OBSERVATION_DATE, COMMON_NAME
         from ebird
         where {region_where_clause}
           and (category = 'species' or category = 'issf' or category = 'form' or common_name = 'Rock Pigeon')
           and observation_date <= '{as_of}'
     ) t
group by observer_id, OBSERVATION_DATE
order by 3 desc, 2 asc, 1 asc
limit {limit};
"""

        logger.debug(f"Generating {title}...")
        a, b = execute_query(sql)
        return title, subtitle, None, a, b

    @staticmethod
    def top_all_time_everyone_day_lists(region_where_clause, as_of, limit=10):
        where = ""
        subtitle = "Biggest Big Day (Everyone)"
        title = subtitle

        sql = f"""
select OBSERVATION_DATE as "Date", count(distinct observer_id) as "Birders", count(distinct t.common_name) as "Species"
from (
         select  OBSERVATION_DATE, COMMON_NAME, observer_id
         from ebird
         where {region_where_clause}
           and (category = 'species' or category = 'issf' or category = 'form' or common_name = 'Rock Pigeon')
           and observation_date <= '{as_of}'
     ) t
group by OBSERVATION_DATE
order by 3 desc
limit {limit};
"""
        logger.debug(f"Generating {title}...")
        a, b = execute_query(sql)
        return title, subtitle, None, a, b

    @staticmethod
    def top_all_time_lists(region_where_clause, as_of, max_hours, max_miles, limit=10):
        title = f"All-Time Top Single List"
        subtitle = f"Biggest List (under {max_hours}h, {max_miles}mi)"
        miles_to_km = 0.6213712

        sql = f"""
select get_observer_name(t.observer_id) as "Observer", 
    min(OBSERVATION_DATE) as "Date", 
    min(locality) as "Locality",
    round(min(duration_minutes) / 60.0, 1) as "Hours", 
    count(t.common_name) as "Species"
from (
         select distinct OBSERVER_ID, SAMPLING_EVENT_IDENTIFIER, OBSERVATION_DATE, duration_minutes, locality, COMMON_NAME
         from ebird
         where {region_where_clause}
           and (category = 'species' or category = 'issf' or category = 'form' or common_name = 'Rock Pigeon')
           and observation_date <= '{as_of}'
           and (duration_minutes is not null and duration_minutes < {max_hours} * 60)
           and (effort_distance_km is null or effort_distance_km*{miles_to_km} < {max_miles})
     ) t
group by observer_id, SAMPLING_EVENT_IDENTIFIER
order by 5 desc
limit {limit};
"""

        logger.debug(f"Generating {title}...")
        a, b = execute_query(sql)
        return title, subtitle, None, a, b

    @staticmethod
    def most_avg_species_per_hour(
        region_where_clause, as_of, year, min_hours=10, min_checklists=10, limit=10
    ):
        where = ""
        title = f"Species/Hour"
        subtitle = f"Average Species Seen Per List-Hour ({year})"

        sql = f"""
select get_observer_name(t.observer_id) as "Observer",
       count(*)                                    as "Lists",
       --max(num_species)                            as max_species_on_list,
       sum(num_species)               as "Sp",
       round(sum(duration_hours), 1)               as "Hours",
       round(avg(num_species / duration_hours), 2) as "Avg Species Per List-Hour"
from (select observer_id, min(duration_minutes) / 60.0 as duration_hours, count(distinct common_name) as num_species
      from ebird
      where true
        and {region_where_clause}
        and (category = 'species' or category = 'issf' or category = 'form' or common_name = 'Rock Pigeon')
        and observation_date <= '{as_of}'
        and duration_minutes is not null
        and duration_minutes >= 5
        and protocol_code in ('P21', 'P22')
        and OBSERVATION_DATE >= '{year}-01-01'
      group by observer_id, SAMPLING_EVENT_IDENTIFIER
     ) t
group by observer_id
having sum(duration_hours) > {min_hours}
   and count(*) > {min_checklists}
order by avg(num_species / duration_hours) desc
limit {limit};
"""
        logger.debug(f"Generating {title}...")
        a, b = execute_query(sql)
        return title, subtitle, None, a, b

    @staticmethod
    def most_honest_birder(
        region_where_clause, as_of, year=None, limit=10, last_x_years=None
    ):
        where = ""
        if year is not None:
            if last_x_years:
                subtitle = f"{year-last_x_years+1}-{year}"
                where += (
                    f" AND extract(year from OBSERVATION_DATE) >= {year-last_x_years+1}"
                )
            else:
                subtitle = f"Most Honest Birder ({year})"
                where += f" AND extract(year from OBSERVATION_DATE) = {year}"

        else:
            subtitle = "Most Honest Birder (All-Time)"

        title = subtitle

        sql = f"""
select get_observer_name(t.observer_id) as "Observer",
       sum(spuhs)                       as "Spuhs",
       sum(slashes)                     as "Slashes",
       sum(total)                       as "Total",
       sum(unq)                      as "Unique"

from (
         select observer_id,
                common_name,
                sum(case when category = 'spuh' then 1 else 0 end)     Spuhs,
                sum(case when category = 'slash' then 1 else 0 end) as Slashes,
                count(*)                                            as Total,
                count(distinct common_name)                         as unq
         from ebird
            where {region_where_clause}
           and category in ('slash', 'spuh')
            and observation_date <= '{as_of}'
           {where}
         group by OBSERVER_ID, COMMON_NAME
     ) t
group by observer_id
order by 4 desc
limit {limit};
"""
        logger.debug(f"Generating {title}...")
        a, b = execute_query(sql)
        return title, subtitle, None, a, b

    @staticmethod
    def new_birds_DEPRECATED(
        region_where_clause, as_of, cur_year_in, prev_year_in, limit=10
    ):
        subtitle = ""

        if prev_year_in == "all":
            prev_where = f" and extract(year from OBSERVATION_DATE) < {cur_year_in} "
            title = f"All-time new in {cur_year_in}"
        else:
            prev_where = f" and extract(year from OBSERVATION_DATE) in ({comma_join(prev_year_in)}) "

            title = f"Reported in {cur_year_in} and not {prev_year_in}"

        subtitle = title

        sql = f"""
select cur.common_name, cur.cnt cur, last.cnt as "last"
from (
         select common_name, count(distinct observer_id) as cnt
         from ebird
         where {region_where_clause}
            and (category = 'species' or category = 'issf' or category = 'form' or common_name = 'Rock Pigeon')
            and observation_date <= '{as_of}'
            and extract(year from OBSERVATION_DATE) in ({comma_join(cur_year_in)})
         group by 1) cur
         left outer join
     (
         select common_name, count(distinct observer_id) as cnt
         from ebird
         where {region_where_clause}
            and (category = 'species' or category = 'issf' or category = 'form' or common_name = 'Rock Pigeon')
            and observation_date <= '{as_of}'
            {prev_where}
         group by 1) last
     on cur.common_name = last.common_name
where last.cnt is null
order by cur.common_name;
--limit {limit};
"""
        logger.debug(f"Generating {title}...")
        a, b = execute_query(sql)
        return title, subtitle, None, a, b

    @staticmethod
    def new_birds(region_where_clause, as_of, year, limit=10):
        title = f"Reported in {year} but Missed in {year-1}"
        subtitle = title

        sql = f"""
select cur.common_name  as "Species",
       (case when last.last_seen is null then 'n/a' else last.last_seen::text end) as "Last Seen",
       cur.min_obs_date as "First Reported",
       cur.cnt          as "Birders",
       (case when has_media = 't' then 'X' else '' end) as "Documented"
from (
         select common_name,
                count(distinct observer_id) as cnt,
                min(observation_date)          min_obs_date,
                max(observation_date)          max_obs_date,
                bool_or(has_media)          as has_media
         from ebird
         where {region_where_clause}
            and (category = 'species' or category = 'issf' or category = 'form' or common_name = 'Rock Pigeon')
            and observation_date <= '{as_of}'
            and observation_date >= '{year}-01-01'
         group by 1) cur
         left outer join
     (
         select common_name, max(observation_date) as last_seen
         from ebird
         where {region_where_clause}
            and (category = 'species' or category = 'issf' or category = 'form' or common_name = 'Rock Pigeon')
            and observation_date <= '{as_of}'
            and observation_date <= '{year-1}-12-31'
         group by 1) last
     on cur.common_name = last.common_name
where last.last_seen < '{year-1}-01-01'
   or last.last_seen is null
order by last_seen asc nulls first;
"""
        logger.debug(f"Generating {title}...")
        a, b = execute_query(sql)
        return title, subtitle, None, a, b


@cache.memoize()
def execute_query(sql):
    with connection.cursor() as cursor:
        cursor.execute(sql)
        vals = namedtuplefetchall(cursor)
        return cursor.description, vals


def get_tabu_format(i, d):
    if i == 0:
        return "X[l]"
    else:
        return "X[-1,r]"

    # SELECT oid, typname
    # FROM pg_catalog.pg_type
    # if d.type_code == 20:  # int8
    #     return "X[-1,r]"
    # if d.type_code == 23:  # int4
    #     return "X[-1,r]"
    #     return "X[-1,r]"
    # if d.type_code in (25, 1043):  # text, varchar
    #     return "X[l]"
    # if d.type_code == 701:  # float8
    #     return "X[-1,r]"
    # if d.type_code == 1700:  # numeric
    #     return "X[-1,r]"
    # raise RuntimeError(d)


def add_tables_in_columns(doc, table_rets, num_columns=3, rank_by_colidx=-1):
    col_width = get_width(num_columns)

    if col_width < 0.4:
        hline_every = None
    else:
        hline_every = 5

    for idx, table_ret in enumerate(table_rets):
        title, subtitle, description, column_desc, vals = table_ret
        # assert subtitle
        # with doc.create(Subsection(subtitle)):
        with doc.create(MiniPage(align="t", pos="t", width=fr"{col_width}\textwidth")):
            if subtitle:
                doc.append(bold(subtitle + "\n"))
            add_table(
                doc,
                column_desc,
                vals,
                rank_by_colidx=rank_by_colidx,
                hline_every=hline_every,
            )

        if idx % num_columns == num_columns - 1:
            doc.append(VerticalSpace("4pt"))
            doc.append(LineBreak(options="4"))
        else:
            doc.append(HorizontalSpace("15pt"))


def add_tables_in(doc, table_rets, columns, rank_by_colidx=-1):
    so_far = 0.0
    for idx, table_ret in enumerate(table_rets):
        num_columns = columns[idx]
        so_far += 1.0 / num_columns
        col_width = get_width(num_columns)

        if col_width < 0.4:
            hline_every = None
        else:
            hline_every = 5

        title, subtitle, description, column_desc, vals = table_ret
        assert subtitle
        # with doc.create(Subsection(subtitle)):
        with doc.create(MiniPage(align="t", pos="t", width=fr"{col_width}\textwidth")):
            doc.append(bold(subtitle + "\n"))
            add_table(
                doc,
                column_desc,
                vals,
                rank_by_colidx=rank_by_colidx,
                hline_every=hline_every,
            )

        if so_far > 0.9:
            doc.append(VerticalSpace("4pt"))
            doc.append(LineBreak(options="4"))
            so_far = 0.0
        else:
            doc.append(HorizontalSpace("15pt"))


def get_width(num_columns):
    if num_columns == 3:
        return 0.31
    elif num_columns == 2:
        return 0.485
    elif num_columns == 1:
        return 1.00
    else:
        raise RuntimeError()


def add_table_section(doc, args):
    """ Add new section with a single table in it. """
    title, subtitle, description, column_desc, vals = args
    with doc.create(Section(title)):
        add_section_description(doc, description)

        add_table(
            doc,
            column_desc,
            vals,
            uselongtabu=True,
            # rank_by_colidx=rank_by_colidx,
            hline_every=5,
        )


def add_list_section(doc, args):
    """ Add new section with a single list in it. """
    title, subtitle, description, column_desc, vals = args
    with doc.create(Section(title)):
        add_section_description(doc, description)

        doc.append(NoEscape(r"\begin{multicols}{3}"))
        # text = []
        for orig_row in vals:
            row = fmtrow(orig_row)
            # assert len(row) == 2
            val = f"{row[0]} ({row[1]})\n"
            doc.append(val)
            if len(row) > 2 and row[2] is not None:
                with doc.create(SmallText()):
                    doc.append(italic("- " + format_list_of_names(row[2]) + "\n"))
                # val += f"\n[{format_list_of_names(row[2])}]"
            # text.append(val + "\n")
        # text = "".join(text)
        # doc.append(text)
        doc.append(NoEscape(r"\end{multicols}"))


def add_table(
    doc, column_desc, vals, uselongtabu=False, rank_by_colidx=-1, hline_every=None
):
    header_row = [d.name for d in column_desc]
    column_def = " ".join([get_tabu_format(i, d) for i, d in enumerate(column_desc)])
    TableClass = LongTabu if uselongtabu else Tabu
    with doc.create(TableClass(column_def, booktabs=True)) as data_table:
        data_table.add_row(header_row, mapper=[bold])
        data_table.add_hline()
        if uselongtabu:
            # data_table.add_empty_row()
            data_table.end_table_header()

        rank = 0
        sort_val = None
        for rowidx, row in enumerate(vals):
            fmtrow = [fmt(x) for x in row]

            if rank_by_colidx is not None and header_row[0].startswith("Observer"):
                current_sort_val = row[rank_by_colidx]

                if sort_val == current_sort_val:
                    pass
                else:
                    rank = rowidx + 1
                    sort_val = current_sort_val
                fmtrow[0] = f"{rank}.\u00A0{fmtrow[0]}"

            data_table.add_row(fmtrow)
            if hline_every:
                # breakpoint()
                if rowidx < len(vals) - 1 and rowidx % hline_every == hline_every - 1:
                    doc.append(NoEscape(r"\hline"))

        doc.append(NoEscape(r"\hline"))


def add_section_description(doc, desc):
    if desc:
        with doc.create(FlushLeft()):
            doc.append(desc)
            doc.append(VerticalSpace("1pt"))
