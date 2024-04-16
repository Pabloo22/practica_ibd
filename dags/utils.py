import os
import pandas as pd


HOUR_BOOLEANS = [f"V{i:02d}" for i in range(1, 25)]
HOUR_VALUES = [f"H{i:02d}" for i in range(1, 25)]
KEEP_COLUMNS = [
    "ESTACION",
    "MAGNITUD",
    "PUNTO_MUESTREO",
    "ANO",
    "MES",
    "DIA",
] + HOUR_VALUES


def extract_from_madrid_url(
    url: str,
    hour_booleans: list[str] | None = None,
    columns_to_keep: list[str] | None = None,
):
    if hour_booleans is None:
        hour_booleans = HOUR_BOOLEANS
    if columns_to_keep is None:
        columns_to_keep = KEEP_COLUMNS

    df = pd.read_csv(url, sep=";", index_col=False)

    for hour_bool in hour_booleans:
        print(f"Check: {hour_bool}")

    df = df[columns_to_keep]

    print("Show 5 first rows: ")
    print(df.head())

    return df


def load_df_to_raw(df: pd.DataFrame, folder_path: str, filename: str):
    file_path = os.path.join(folder_path, filename)

    # Write DataFrame to CSV
    df.to_csv(file_path, index=False)
    print(f"DataFrame written to {file_path}")
