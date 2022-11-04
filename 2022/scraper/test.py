from functions import sdat_scraper as sdat
import pandas as pd

suffix_list = sdat.load_suffixes("data/street_name_suffixes.csv")
locations = sdat.load_locations(f"data/montgomery_locations.csv")
print(f"LOCATIONS RAW : {len(locations)}")
search_terms = sdat.remove_suffix(locations, suffix_list)
search_terms = sorted(search_terms)
dict_ = {
    "Name" : search_terms
}

df = pd.DataFrame(dict_)
df.to_csv(f"test.csv")
print(search_terms)