# EDA - Univariate Analysis

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import os

# --------------------------- SETUP ---------------------------

# Ensure summary_stats folder exists
IMAGE_DIR = "../../report/images/summary_stats"
os.makedirs(IMAGE_DIR, exist_ok=True)

def save_plot(name):
    """Save current matplotlib figure into images/ as PNG."""
    filepath = os.path.join(IMAGE_DIR, f"{name}.png")
    plt.savefig(filepath, dpi=300, bbox_inches='tight')
    print(f"Saved: {filepath}")


df = pd.read_csv("../../data/processed/cleaned_final_dataset.csv")

sns.set_theme(
    style="whitegrid",
    context="notebook",
    palette="colorblind",
    font_scale=1.1
)
df.head() # shows the first 5 rows of the dataset
df.tail() # shows the 5 last rows preview
df.shape  # shows the dimension of the dataset
df.dtypes  # check the structure of the data
df.isna().sum() # check if there is some missing value (no missing values found)

# --------------------------- DISTRIBUTION OF POPULARITY ---------------------------

plt.figure(figsize=(10, 5))
sns.histplot(df["track_popularity"], kde=True, bins=30)
plt.title("Distribution of track popularity")
plt.xlabel("Popularity")
plt.ylabel("Count")
save_plot("distribution_track_popularity")
plt.show()

# --------------------------- GENRE MAPPING & DISTRIBUTION ---------------------------

def map_genre(genre):
    if pd.isna(genre) or str(genre).strip().lower() in ["n/a", "unknown", ""]:
        return None

    g = genre.lower()

    if "rock" in g:
        return "Rock"
    if "pop" in g or "christmas" in g:
        return "Pop"
    if "hip hop" in g or "hip-hop" in g or "rap" in g:
        return "Hip-Hop/Rap"
    if any(x in g for x in ["edm", "electronic", "electro", "new wave", "synth"]):
        return "Electronic/Synth"
    if any(x in g for x in ["jazz", "motown", "northern soul", "new jack swing"]):
        return "Jazz"
    if "metal" in g:
        return "Metal"
    if "folk" in g or "country" in g:
        return "Country"
    if any(x in g for x in ["r&b", "soul", "doo-wop", "doowop"]):
        return "R&B/Soul"

    return "Other"


df["genre_grouped"] = df["genre"].apply(map_genre)
genre_counts = df["genre_grouped"].value_counts(dropna=True)

plt.figure(figsize=(10, 6))
plt.bar(genre_counts.index, genre_counts.values)
plt.xticks(rotation=45, ha="right")
plt.title("Genre Distribution")
plt.xlabel("Genre")
plt.ylabel("Count")
plt.tight_layout()
save_plot("distribution_genre")
plt.show()

# --------------------------- DISTRIBUTION OF GENDER ---------------------------

plt.figure(figsize=(8, 5))
sns.countplot(data=df, x="gender")
plt.title("Distribution of Gender")
plt.xlabel("Gender")
plt.ylabel("Count")
save_plot("distribution_gender")
plt.show()

# --------------------------- MALE VS FEMALE ONLY ---------------------------

df_binary_gender = df[df["gender"].isin(["male", "female"])]

plt.figure(figsize=(8, 5))
sns.countplot(data=df_binary_gender, x="gender")
plt.title("Distribution of Gender (Male vs Female)")
plt.xlabel("Gender")
plt.ylabel("Count")
save_plot("distribution_gender_binary")
plt.show()

# --------------------------- RELEASE YEAR ---------------------------

plt.figure(figsize=(10, 5))
sns.histplot(df["release_year"], bins=5)
plt.title("Distribution of Release Years")
plt.xlabel("Year")
plt.ylabel("Count")
save_plot("distribution_release_year")
plt.show()

# --------------------------- DECADE ---------------------------

df["decade"] = (df["release_year"] // 10) * 10
df["decade"] = df["decade"].astype(int).astype(str) + "s"

plt.figure(figsize=(10, 5))
sns.countplot(data=df, x="decade", order=sorted(df["decade"].unique()))
plt.title("Distribution of Tracks by Decade")
plt.xlabel("Decade")
plt.ylabel("Count")
save_plot("distribution_decade")
plt.show()
