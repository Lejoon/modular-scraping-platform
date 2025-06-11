import csv
from datetime import datetime, timedelta

# Existing data about sets
sets = [
    {
        "Set Name": "Spark of Rebellion",
        "Release Date": "2024-03-08",
        "TCGPlayer Booster Product ID": "533898",
        "TCGPlayer Booster Box Product ID": "533897",
        "TCGPlayer Group ID": "533897",
    },
    {
        "Set Name": "Shadows of the Galaxy",
        "Release Date": "2024-07-12",
        "TCGPlayer Booster Product ID": "549700",
        "TCGPlayer Booster Box Product ID": "549696",
        "TCGPlayer Group ID": "549696",
    },
    {
        "Set Name": "Twilight of the Republic",
        "Release Date": "2024-11-08",
        "TCGPlayer Booster Product ID": "578940",
        "TCGPlayer Booster Box Product ID": "578939",
        "TCGPlayer Group ID": "578939",
    },
    {
        "Set Name": "Jump to Lightspeed",
        "Release Date": "2025-03-14",
        "TCGPlayer Booster Product ID": "610306",
        "TCGPlayer Booster Box Product ID": "610308",
        "TCGPlayer Group ID": "610308",
    },
    {
        "Set Name": "Legends of the Force",
        "Release Date": "2025-07-11",
        "TCGPlayer Booster Product ID": "626542",
        "TCGPlayer Booster Box Product ID": "626543",
        "TCGPlayer Group ID": "626543",
    },
]

def compute_shifted(date_str, weeks_shift):
    """Return shifted date string and quarter label."""
    d = datetime.strptime(date_str, "%Y-%m-%d")
    shifted = d + timedelta(weeks=weeks_shift)
    quarter = (shifted.month - 1) // 3 + 1
    return f"{shifted.date()} ({shifted.year} Q{quarter})"

# Prepare rows with additional columns
output_rows = []
for s in sets:
    revenue = compute_shifted(s["Release Date"], -2)  # 2 weeks earlier
    cashflow = compute_shifted(s["Release Date"], 8)  # 8 weeks later
    output_rows.append([
        s["Set Name"],
        s["Release Date"],
        s["TCGPlayer Booster Product ID"],
        s["TCGPlayer Booster Box Product ID"],
        s["TCGPlayer Group ID"],
        revenue,
        cashflow,
        "",  # trailing blank column
    ])

# Write to CSV
header = [
    "Set Name",
    "Release Date",
    "TCGPlayer Booster Product ID",
    "TCGPlayer Booster Box Product ID",
    "TCGPlayer Group ID",
    "First Revenue",
    "First Cashflow",
    "",
]
file_path = "starwars_sets.csv"
with open(file_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f, delimiter=";")
    writer.writerow(header)
    writer.writerows(output_rows)
# saved file to
file_path
