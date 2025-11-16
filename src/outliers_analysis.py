import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Load your dataset (adjust the path)
data = pd.read_csv("data/processed/cleaned_final_dataset.csv")

# Function to detect outliers using IQR
def detect_outliers(df, col):
    Q1 = df[col].quantile(0.25)
    Q3 = df[col].quantile(0.75)
    IQR = Q3 - Q1
    lower = Q1 - 1.5 * IQR
    upper = Q3 + 1.5 * IQR
    return df[(df[col] < lower) | (df[col] > upper)]

# Example: highlight outliers in track_popularity vs duration_ms
outliers = detect_outliers(data, "duration_ms")  # we focus on duration_ms here

plt.figure(figsize=(10,6))
sns.scatterplot(data=data, x="duration_ms", y="track_popularity", alpha=0.5, label="Normal")
sns.scatterplot(data=outliers, x="duration_ms", y="track_popularity", color="red", label="Outliers", s=60)
plt.title("Track Popularity vs Duration (Outliers Highlighted)")
plt.xlabel("Duration (ms)")
plt.ylabel("Popularity (0-100)")
plt.legend()
plt.show()
