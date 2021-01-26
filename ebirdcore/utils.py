import logging
import requests
from bs4 import BeautifulSoup
from .models import EBird
from diskcache import Cache

cache = Cache("cache_observer_name")

logger = logging.getLogger(__name__)


@cache.memoize()
def get_observer_name(obs_id):
    # checklist_code = "S69886809"
    e = EBird.objects.filter(observer_id=obs_id).first()

    checklist_code = e.sampling_event_identifier
    r = requests.get(f"https://ebird.org/checklist/{checklist_code}")
    soup = BeautifulSoup(r.text, "html.parser")
    ret = [
        item
        for item in soup("meta")
        if item.has_attr("name") and item.attrs["name"].lower() == "author"
    ][0]["content"]

    logger.debug(f"Identified {obs_id} as {ret} via checklist {checklist_code}.")

    return ret

