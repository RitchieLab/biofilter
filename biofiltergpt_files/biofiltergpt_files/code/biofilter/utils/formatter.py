import pandas as pd


def orm_list_to_df(results):
    """
    Convert a list of SQLAlchemy ORM instances to a pandas DataFrame.

    Args:
        results (List[Base]): List of SQLAlchemy ORM objects.

    Returns:
        pd.DataFrame: Cleaned DataFrame with ORM fields.
    """
    if not results:
        return pd.DataFrame()
    return pd.DataFrame([r.__dict__ for r in results]).drop(
        columns=["_sa_instance_state"], errors="ignore"
    )
