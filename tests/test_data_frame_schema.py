from mipdb.data_frame_schema import DataFrameSchema
from mipdb.exceptions import InvalidDatasetError
import pytest
import pandas as pd

dataframes = [
    pytest.param(
        pd.DataFrame(
            {
                "var4": [22, None],
                "dataset": ["dataset1", "dataset1"],
            },
        ),
        {"dataset": "text", "var4": "int"},
        {"var3": (10, None)},
        {"dataset": ["dataset1", "dataset2"]},
        id="validate with nan values integer column with minValue",
    ),
    pytest.param(
        pd.DataFrame(
            {
                "var4": [1, None],
                "dataset": ["dataset1", "dataset1"],
            }
        ),
        {"dataset": "text", "var4": "int"},
        {"var4": (None, 100)},
        {"dataset": ["dataset1", "dataset2"]},
        id="validate with nan values integer column with only maxValue",
    ),
    pytest.param(
        pd.DataFrame(
            {
                "var4": [1, None],
                "dataset": ["dataset1", "dataset1"],
            }
        ),
        {"dataset": "text", "var4": "int"},
        {"var4": (None, None)},
        {"dataset": ["dataset1", "dataset2"]},
        id="validate with nan values integer column without min max",
    ),
]


@pytest.mark.parametrize(
    "dataframe,sql_type_per_column,cdes_with_min_max,cdes_with_enumerations", dataframes
)
def test_validate_dataframe(
    dataframe, sql_type_per_column, cdes_with_min_max, cdes_with_enumerations
):
    dataframe_schema = DataFrameSchema(
        sql_type_per_column,
        cdes_with_min_max,
        cdes_with_enumerations,
        dataframe.columns.to_list(),
    )
    dataframe_schema.validate_dataframe(dataframe=dataframe)


invalid_dataframes = [
    pytest.param(
        pd.DataFrame(
            {
                "var4": [1, 1.1],
                "dataset": ["dataset1", "dataset1"],
            }
        ),
        "An error occurred while validating the csv on column: 'var4'",
        id="int with float",
    ),
    pytest.param(
        pd.DataFrame(
            {
                "var3": [1.1, "not a float"],
                "dataset": ["dataset1", "dataset1"],
            }
        ),
        "An error occurred while validating the csv on column: 'var3'",
        id="float with text",
    ),
    pytest.param(
        pd.DataFrame(
            {
                "var4": [1, "not a int"],
                "dataset": ["dataset1", "dataset1"],
            }
        ),
        "An error occurred while validating the csv on column: 'var4'",
        id="int with text",
    ),
    pytest.param(
        pd.DataFrame(
            {
                "var2": ["1", "l1"],
                "dataset": ["dataset1", "dataset1"],
            }
        ),
        "An error occurred while validating the csv on column: 'var2'",
        id="text with non existing enumeration",
    ),
    pytest.param(
        pd.DataFrame(
            {
                "var5": [1.0, 2.0],
                "dataset": ["dataset1", "dataset1"],
            }
        ),
        "The column: 'var5' does not exist in the metadata",
        id="text with int/float enumerations(1,2.0)  and 1.0 was given",
    ),
    pytest.param(
        pd.DataFrame(
            {
                "var3": [4, 5],
                "dataset": ["dataset1", "dataset1"],
            }
        ),
        "An error occurred while validating the csv on column: 'var3'",
        id="enumeration exceeds min",
    ),
    pytest.param(
        pd.DataFrame(
            {
                "var3": [5, 65],
                "dataset": ["dataset1", "dataset1"],
            }
        ),
        "An error occurred while validating the csv on column: 'var3'",
        id="enumeration exceeds max",
    ),
]


@pytest.mark.parametrize("dataframe,exception_message", invalid_dataframes)
def test_invalid_dataset_error_cases(dataframe, exception_message):
    sql_type_per_column = {
        "var1": "text",
        "subjectcode": "text",
        "var2": "text",
        "dataset": "text",
        "var3": "real",
        "var4": "int",
    }
    cdes_with_min_max = {"var3": (5, 60)}
    cdes_with_enumerations = {
        "var2": ["l1", "l2"],
        "dataset": ["valid_dataset", "dataset_is_not_unique"],
    }

    with pytest.raises(InvalidDatasetError, match=exception_message):
        dataframe_schema = DataFrameSchema(
            sql_type_per_column,
            cdes_with_min_max,
            cdes_with_enumerations,
            dataframe.columns.to_list(),
        )
        dataframe_schema.validate_dataframe(dataframe=dataframe)
