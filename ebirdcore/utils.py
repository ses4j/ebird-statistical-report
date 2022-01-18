import datetime
import logging
import requests
from bs4 import BeautifulSoup
from .models import EBird
from diskcache import Cache

cache = Cache("cache_observer_name")

logger = logging.getLogger(__name__)


def get_checklist_url(sampling_event_identifier):
    return f"https://ebird.org/checklist/{sampling_event_identifier}"


@cache.memoize()
def get_observer_name(obs_id):
    # checklist_code = "S69886809"
    logger.warning(f"Finding {obs_id}...")
    e = EBird.objects.filter(observer_id=obs_id).first()

    checklist_code = e.sampling_event_identifier
    url = get_checklist_url(checklist_code)
    logger.debug(f"...fetching {url}")
    r = requests.get(url)
    soup = BeautifulSoup(r.text, "html.parser")
    ret = [
        item
        for item in soup("meta")
        if item.has_attr("name") and item.attrs["name"].lower() == "author"
    ][0]["content"]

    logger.debug(f"Identified {obs_id} as {ret} via checklist {checklist_code}.")

    return ret



def add_years(d, years):
    """Return a date that's `years` years after the date (or datetime)
    object `d`. Return the same calendar date (month and day) in the
    destination year, if it exists, otherwise use the following day
    (thus changing February 29 to March 1).

    http://stackoverflow.com/a/15743908/237091
    """
    try:
        return d.replace(year=d.year + years)
    except ValueError:
        return d + (datetime.date(d.year + years, 1, 1) - datetime.date(d.year, 1, 1))


def parse_region_code(region_code):
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

    return {
        "code": region_code,
        "description": region_description,
        "example_obj": one_record,
        "where_clause": region_where_clause,
    }


MR_CODES = (
    "M1TO12",
    "M3TO5",
    "M6TO7",
    "M8TO11",
    "M12TO2",
)

SEX_CODES = ("", "m", "f")
AGE_CODES = ("", "a", "i", "j")


def get_top_ranked_photo(
    region_code,
    region_description=None,
    common_name=None,
    year=None,
    taxon_code=None,
    mr="M1TO12",
    sex="",
    ages=None,
    exclude=None,
):
    import requests, shutil

    if not ages:
        ages = []

    if year is None:
        by = 1900
        this_year = datetime.datetime.now().date().year
        ey = this_year
    else:
        by = ey = year

    """
    https://ebird.org/media/catalog.json?searchField=user&q=&taxonCode=easowl1&hotspotCode=&regionCode=US&customRegionCode=&userId=&_mediaType=&mediaType=p
    &species=Eastern+Screech-Owl+-+Megascops+asio&region=&hotspot=&customRegion=
    &mr=M3TO5&bmo=1&emo=12&yr=YPAST10&by=1900&ey=2021&user=&view=Gallery&sort=upload_date_desc&age=a&sex=m&_req=&
    """
    url = f"https://ebird.org/media/catalog.json?searchField=user&q=&regionCode={region_code}&mediaType=p&hotspot=&customRegion=&mr={mr}&bmo=1&emo=12&yr=YCUSTOM&by={by}&ey={ey}&user=&view=Gallery&sort=rating_rank_desc&includeUnconfirmed=T&_req=&cap=no&subId=&catId=&_spec=&specId=&collectionCatalogId=&dsu=-1&action=reset_status&start=0"
    if taxon_code:
        url += f"&taxonCode={taxon_code}&species={common_name or taxon_code}"
    if sex:
        url += f"&sex={sex}"
    for age in ages:
        url += f"&age={age}"

    r = requests.get(url)
    print(url)

    result_json = r.json()
    if "results" not in result_json:
        breakpoint()
        return

    count = result_json["results"]["count"]
    if count == 0:
        print(taxon_code, age, sex, "count is 0")
        return None
    #     print(url)
    # # breakpoint()
    #         return None
    # except:
    # breakpoint()
    # return None

    for photo_data in result_json["results"]["content"]:
        if exclude:
            matches_exclude = False
            for excl in exclude:
                if excl and photo_data["assetId"] == excl["asset_id"]:
                    matches_exclude = True
                    break
            if matches_exclude:
                continue
        break
    # breakpoint()
    photo_url = photo_data["mediaUrl"]
    common_name = photo_data["commonName"]
    user_display_name = photo_data["userDisplayName"]
    img_response = requests.get(photo_url, stream=True)
    img_filename = f"images/top-image-{taxon_code or 'all'}-{region_code}-{year or 'all'}-{sex or 'all'}-{''.join(ages) or 'all'}.jpg"
    with open(img_filename, "wb") as out_file:
        shutil.copyfileobj(img_response.raw, out_file)
    del img_response
    if region_description:
        caption = f"{common_name} - The top-rated photo in {region_description} for {year} - ©{year} {user_display_name}"
    else:
        caption = f"{common_name} - ©{year} {user_display_name}"
    print(
        img_filename,
        caption,
    )
    return {
        "caption": caption,
        "image_filename": img_filename,
        "media_url": photo_url,
        "age": photo_data.get("age"),
        "sex": photo_data.get("sex"),
        "rating": photo_data.get("rating"),
        "location_line1": photo_data.get("locationLine1"),
        "asset_id": photo_data.get("assetId"),
        "user_display_name": photo_data.get("userDisplayName"),
        "width": photo_data.get("width"),
        "height": photo_data.get("height"),
        "sci_name": photo_data.get("sci_name"),
        "count": count,
    }
