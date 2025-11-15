import time
import requests
import pandas as pd
from urllib.parse import quote_plus

# ============================================================
# MusicBrainz Query
# ============================================================

def query_musicbrainz(url: str):
    try:
        r = requests.get(
            url,
            headers={"User-Agent": "LocalDatasetExtractor/1.0"},
            timeout=8
        )
        if r.status_code == 200:
            return r.json()
        return None
    except:
        return None

def get_artist_gender(artist_name: str):
    if not artist_name:
        return None

    encoded = quote_plus(f'artist:"{artist_name}"')
    url = f"https://musicbrainz.org/ws/2/artist/?query={encoded}&fmt=json&limit=1"

    data = query_musicbrainz(url)
    if not data or "artists" not in data or len(data["artists"]) == 0:
        return None

    return data["artists"][0].get("gender")

# ============================================================
# Guaranteed Working Parser
# ============================================================

def parse_artist_field(raw):
    """Parse semicolon-separated or single artist."""
    if not isinstance(raw, str):
        return []

    if ";" in raw:
        return [a.strip() for a in raw.split(";") if a.strip()]
    return [raw.strip()] if raw.strip() else []

# ============================================================
# Main Processing
# ============================================================

def enrich_dataset(input_csv_path: str, output_csv_path: str):

    df = pd.read_csv(input_csv_path)

    genders = []

    for raw in df["artist_names"]:
        print("RAW =", repr(raw))

        artists = parse_artist_field(raw)
        print("PARSED =", artists)

        first_artist = artists[0] if artists else None
        print("FIRST ARTIST =", first_artist)

        gender = get_artist_gender(first_artist)
        print(f"→ {first_artist} → gender = {gender}\n")

        genders.append(gender)
        time.sleep(1)

    df["gender"] = genders
    df.to_csv(output_csv_path, index=False)
    print("Saved:", output_csv_path)

def main():
    enrich_dataset(
        "../data/raw/cleaned_top_songs-with_genres.csv",
        "data/raw/cleaned_top_songs-with_genres-genders.csv"
    )

if __name__ == "__main__":
    main()
