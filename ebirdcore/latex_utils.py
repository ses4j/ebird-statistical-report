from ebirdcore.utils import get_checklist_url
from ebirdcore.sql_utils import fmt, fmtrow, format_list_of_names
import logging
from pylatex.base_classes.containers import Environment
from pylatex.basic import SmallText
from pylatex.package import Package
from diskcache import Cache

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
    Table,
    LongTabularx,
    Tabularx,
    TextColor,
    TikZ,
    VerticalSpace,
    simple_page_number,
)
from pylatex.position import Center, FlushLeft, FlushRight
from pylatex.utils import NoEscape, bold, escape_latex, italic


cache = Cache("cachedir")
logger = logging.getLogger(__name__)


def get_tabular_format(i, d):
    if i == 0:
        return "X"
    else:
        return "r"

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
            # if subtitle:
            #     doc.append(bold(subtitle))
            add_table(
                doc,
                column_desc,
                vals,
                rank_by_colidx=rank_by_colidx,
                hline_every=hline_every,
                caption=subtitle
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
            # doc.append(bold(subtitle))
            add_table(
                    doc,
                    column_desc,
                    vals,
                    rank_by_colidx=rank_by_colidx,
                    hline_every=hline_every,
                    caption=subtitle
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
    doc, column_desc, vals, uselongtabu=False, rank_by_colidx=-1, hline_every=None, caption=None
):
    def filter_private_cols(row):
        return [v for d, v in zip(column_desc, row) if not d.name.startswith("_")]

    header_row = [d.name for d in column_desc if not d.name.startswith("_")]
    column_def = " ".join(
        [
            get_tabular_format(i, d)
            for i, d in enumerate(column_desc)
            if not d.name.startswith("_")
        ]
    )
    TableClass = LongTabularx if uselongtabu else Tabularx
    # with doc.create(Table()) as table:
        # if caption:
        #     table.add_caption(caption)
    if caption:
        doc.append(bold(caption))
    with doc.create(TableClass(column_def, booktabs=True)) as data_table:
        data_table.add_row(header_row, mapper=[bold])
        data_table.add_hline()
        if uselongtabu:
            # data_table.add_empty_row()
            data_table.end_table_header()

        rank = 0
        sort_val = None
        for rowidx, row in enumerate(vals):
            filtered_row = filter_private_cols(row)
            fmtrow = [fmt(x) for x in filtered_row]

            if rank_by_colidx is not None and header_row[0].startswith("Observer"):
                current_sort_val = filtered_row[rank_by_colidx]

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
