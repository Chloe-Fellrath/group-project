import os
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

# Ensure summary_stats folder exists
IMAGE_DIR = "../../report/images/summary_stats"
os.makedirs(IMAGE_DIR, exist_ok=True)


def save_plot(name):
    """Save current matplotlib figure into images/ as PNG."""
    filepath = os.path.join(IMAGE_DIR, f"{name}.png")
    plt.savefig(filepath, dpi=300, bbox_inches='tight')
    print(f"Saved: {filepath}")


# Load dataset
df = pd.read_csv("../../data/processed/cleaned_final_dataset.csv")

key_vars = ["release_year", "genre", "gender", "track_popularity"]

# -----------------------------
# SUMMARY STATISTICS
# -----------------------------
for var in key_vars:
    print(f"\n=== Summary Statistics for {var} ===")

    if pd.api.types.is_numeric_dtype(df[var]):
        # Numeric variables
        summary_stats = df[var].describe()
        print(summary_stats.to_string())
        print(f"Skewness: {df[var].skew():.3f}")
        print(f"Kurtosis: {df[var].kurtosis():.3f}")
    else:
        # Categorical variables
        counts = df[var].value_counts(dropna=False)  # include NaNs
        print("\nCounts:")
        print(counts.to_string())
        print(f"Unique categories: {df[var].nunique()}")

# Function to generate summary stats for categorical variables
def categorical_summary(df, columns):
    summary = {}
    for col in columns:
        counts = df[col].value_counts()
        percentages = df[col].value_counts(normalize=True) * 100
        summary[col] = pd.DataFrame({'Count': counts, 'Percentage': percentages})
    return summary

# Generate summary for 'genre' and 'gender'
summary_stats = categorical_summary(df, ['genre', 'gender'])

# Display the results
for col, stats in summary_stats.items():
    print(f"Summary for {col}:\n{stats}\n")

# -----------------------------
# BOX PLOT FOR NUMERIC VARIABLES
# -----------------------------
numeric_vars = ["release_year", "track_popularity"]

for var in numeric_vars:
    plt.figure(figsize=(6, 4))
    sns.boxplot(y=df[var], color='lightblue')
    plt.title(f"{var} Distribution (Box Plot)", fontsize=13, fontweight='bold', pad=15)
    plt.ylabel(var, fontsize=11)
    plt.grid(True, axis='y', alpha=0.3, linestyle='--')
    plt.tight_layout()
    save_plot(f"boxplot_{var}")
    plt.show()

# -----------------------------
# BAR PLOTS FOR CATEGORICAL VARIABLES
# -----------------------------
categorical_vars = ["genre", "gender"]


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


# Apply genre mapping
df["genre_grouped"] = df["genre"].apply(map_genre)

for var in categorical_vars:
    plt.figure(figsize=(8, 4))
    if var == "genre":
        counts = df["genre_grouped"].value_counts(dropna=True)
    else:
        counts = df[var].value_counts(dropna=True)

    sns.barplot(x=counts.index, y=counts.values, palette="pastel")
    plt.xticks(rotation=45, ha="right")
    plt.title(f"{var} Distribution", fontsize=13, fontweight="bold")
    plt.ylabel("Count")
    plt.xlabel(var)
    plt.tight_layout()
    save_plot(f"barplot_{var}")
    plt.show()
