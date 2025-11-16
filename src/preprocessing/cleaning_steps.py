import pandas as pd

df = pd.read_csv("data/raw/cleaned_top_songs-with_genres-genders.csv")
# Remove spaces and convert to lowercase
df['artist'] = df['artist_names'].str.strip().str.title()
df['track_name'] = df['track_name'].str.strip().str.lower()

# ==========================
# Check duplicates based on specific columns
# Example: track_id, track_name, artist_names
# ==========================
key_columns = ["track_id", "track_name", "artist_names"]
key_duplicates = df[df.duplicated(subset=key_columns, keep=False)]
print(f"\nNumber of duplicate tracks (based on {key_columns}): {len(key_duplicates)}")
if len(key_duplicates) > 0:
    print(key_duplicates)

def clean_missing_values(df):
    # Text
    text_cols = ['artist_names', 'track_name', 'album_name', 'genre', 'album_release_date', 'gender']
    for col in text_cols:
        df[col] = df[col].fillna('unknown').str.lower()

    # Numeric
    numeric_cols = ['track_popularity', 'duration_ms', 'album_total_tracks', 'release_year']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].fillna(0)
            df[col] = df[col].astype('int64')  # or: df[col] = df[col].apply(np.int64)

    # Mark groups in gender
    df.loc[df['artist_names'].str.contains(';'), 'gender'] = 'group'
    return df

df = pd.read_csv("data/raw/cleaned_top_songs-with_genres-genders.csv")
df = clean_missing_values(df)

print(df.head())  # shows the first 5 rows
print(df.info())  # shows column types and missing counts
print(df.isnull().sum())
print(df['release_year'].unique())  # Make sure year column looks correct

