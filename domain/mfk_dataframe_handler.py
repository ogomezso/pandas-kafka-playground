import pandas as pd

from model import mfk_row as model


def create_dataframe_from_path(path, separator=";"):
    return pd.read_csv(path, separator)


def create_model_object(df):
    return [(model.MfkRow(row.INSTITUT_A, row.PERSONEN_NUMMER_A, row.INSTITUT_B, row.PERSONEN_NUMMER_B)) for
            index, row in df.iterrows()]
