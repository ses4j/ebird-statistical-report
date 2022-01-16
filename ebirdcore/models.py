import datetime
from django.db import models
from django.contrib.gis.db.models import PointField


class EBird(models.Model):
    class Meta:
        app_label = "ebird"
        db_table = "ebird"
        # unique_together = (('account', 'process_date'))

    global_unique_identifier = models.CharField(
        primary_key=True, max_length=50
    )  # always 45-47 characters needed (so far)
    category = models.CharField(max_length=20)  # probably 10 would be safe
    common_name = models.TextField()  # some hybrids have really long names
    scientific_name = models.TextField()
    subspecies_common_name = models.TextField()
    # observation_count_str = models.CharField(
    #     max_length=8
    # )  # someone saw 1.3 million auklets.
    observation_count = models.IntegerField(
        null=True,
        help_text="set to integer version of observation_count if numberable.",
    )  # someone saw 1.3 million auklets.
    # unfortunately, it can't be an= models.IntegerField()eger
    # because some are just presence/absence
    behavior_code = models.CharField(max_length=2)
    breeding_code = models.CharField(max_length=2)
    breeding_category = models.CharField(max_length=2)
    country = models.TextField()
    country_code = models.CharField(max_length=2)  # alpha-2 codes
    state = models.TextField()
    state_code = models.CharField(max_length=30)
    county = models.TextField()
    county_code = models.CharField(max_length=30)

    atlas_block = models.CharField(max_length=20)  # i think max 10
    locality = models.TextField()  # unstructured/potentially long
    locality_id = models.CharField(max_length=10)  # maximum observed so far is 8
    locality_type = models.CharField(max_length=2)  # short codes
    latitude = models.FloatField()  # is this the appropriate level of precision?
    longitude = models.FloatField()  #    ''
    observation_date = models.DateField()  # do i need to specify ymd somehow?
    observation_doy = models.PositiveSmallIntegerField()
    time_observations_started = models.TimeField()  # how do i make this a time?
    observer_id = models.CharField(
        max_length=12
    )  # max of 9 in the data i've seen so far
    sampling_event_identifier = models.CharField(
        max_length=12
    )  # probably want to index on this.
    protocol_code = models.CharField(max_length=5)
    project_code = models.CharField(max_length=20)  # needs to be at least 10 for sure.
    duration_minutes = models.IntegerField()  # bigint?
    effort_distance_km = models.FloatField()  # precision?
    effort_area_ha = models.FloatField()  # precision?
    number_observers = models.IntegerField()  # just a small= models.IntegerField()
    all_species_reported = (
        models.IntegerField()
    )  # seems to always be 1 or 0.  maybe i could make this boolean?
    group_identifier = models.CharField(max_length=10)  # appears to be max of 7 or 8
    has_media = models.BooleanField()
    approved = models.BooleanField()  # can be boolean?
    reviewed = models.BooleanField()  # can be boolean?
    # may need to be longer if data set includes unvetted data
    reason = models.TextField()

    trip_comments = models.TextField()  # comments are long, unstructured,
    species_comments = models.TextField()

    geog = PointField(null=True, blank=True, geography=True)

    # objects = DataFrameManager()

    def get_start_datetime(self):
        return datetime.datetime.combine(
            self.observation_date, self.time_observations_started
        )

    def get_observer(self):
        key = self.observer_id.strip()
        return _obs.get(key, key)

    def get_atlas_code_desc(self):
        if self.breeding_code is None:
            return None
        c = breeding_codes.get(self.breeding_code.strip(), "???")
        return c.split("–", 1)[0]

    def get_breeding_category_short_desc(self):
        if self.breeding_category == "C1":
            return "Obs"
        if self.breeding_category == "C2":
            return "Poss"
        if self.breeding_category == "C3":
            return "Prob"
        if self.breeding_category == "C4":
            return "Conf"
        return self.breeding_category


breeding_codes = {
    "NY": "Nest with Young – Nest with young seen or heard. Typically considered Confirmed.",
    "NE": "Nest with Eggs – Nest with eggs. Typically considered Confirmed.",
    "FS": "Carrying Fecal Sac – Adult carrying fecal sac. Typically considered Confirmed.",
    "FY": "Feeding Young – Adult feeding young that have left the nest, but are not yet flying and independent (should not be used with raptors, terns, and other species that may move many miles from the nest site). Typically considered Confirmed.",
    "CF": "Carrying Food – Adult carrying food for young (should not be used for corvids, raptors, terns, and certain other species that regularly carry food for courtship or other purposes). Typically considered Confirmed.",
    "FL": "Recently Fledged young – Recently fledged or downy young observed while still dependent upon adults. Typically considered Confirmed.",
    "ON": "Occupied Nest – Occupied nest presumed by parent entering and remaining, exchanging incubation duties, etc. Typically considered Confirmed.",
    "UN": 'Used nest – Unoccupied nest, typically with young already fledged and no longer active, observed and conclusively identified as belonging to the entered species; note that this breeding code may accompany a count of "0" if no live birds were seen/heard on the checklist. Typically considered Confirmed.',
    "DD": "Distraction Display – Distraction display, including feigning injury. Typically considered Confirmed.",
    "NB": "Nest Building – Nest building at apparent nest site (should not be used for certain wrens, and other species that build dummy nests). Typically considered Confirmed, sometimes Probable.",
    "CN": "Carrying Nesting Material – Adult carrying nesting material; nest site not seen. Typically considered Confirmed, sometimes Probable.",
    "PE": "Brood Patch and Physiological Evidence – Physiological evidence of nesting, usually a brood patch. This will be used only very rarely. Typically considered Confirmed.",
    "B": "Woodpecker/Wren nest building – Nest building at apparent nest site observed in Woodpeckers (Family: Picidae) or Wrens (Family: Troglodytidae)—both species known to built dummy nests or roost cavities. Typically considered Probable.",
    "T": "Territory held for 7+ days – Territorial behavior or singing male present at the same location 7+ days apart. Typically considered Probable.",
    "A": 'Agitated behavior – Agitated behavior or anxiety calls from an adult (ex. "pishing" and strong tape responses). Typically considered Probable.',
    "N": "Visiting probable Nest site – Visiting repeatedly probable nest site (primarily hole nesters). Typically considered Probable.",
    "C": "Courtship, Display or Copulation – Courtship or copulation observed, including displays and courtship feeding. Typically considered Probable.",
    "T": "Territory held for 7+ days – Territorial behavior or singing male present at the same location 7+ days apart. Typically considered Probable.",
    "P": "Pair in suitable habitat – Pair observed in suitable breeding habitat within breeding season. Typically considered Probable.",
    "M": "Multiple (7+) singing males. Count of seven or more signing males observed in a given area. Typically considered probable.",
    "S7": "Singing male present 7+ days – Singing male, presumably the same individual, present in suitable nesting habitat during its breeding season and holding territory in the same area on visits at least 7 days apart. Typically considered probable.",
    "S": "Singing male – Singing male present in suitable nesting habitat during its breeding season. Typically considered Possible.",
    "H": "In appropriate habitat – Adult in suitable nesting habitat during its breeding season. Typically considered Possible.",
    "F": "Flyover – Flying over only. This is not necessarily a breeding code, but can be a useful behavioral distinction.",
}
