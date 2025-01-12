from typing import Any

import numpy as np
from rich.console import Console
from rich.table import Table, box


def table(
    console: Console,
    title: str,
    dictionary: dict[str, list | Any],
    render: bool = True,
) -> Table:
    """Create a table from a dictionary.

    Uses the rich library to create a table from a dictionary where the keys are
    the column names and the values are the rows.

    Args:
        console (Console): rich console
        title (str): Title of the table
        dictionary (dict[str, list | Any]): Dictionary with column names as keys
            and lists as values
        render (bool, optional): Wether to print the console to terminal or not.
            Defaults to True.

    Returns:
        Table: rich table object with the data from the dictionary
    """
    table = Table(title=title, box=box.ROUNDED)

    keys = np.array(list(dictionary.keys()))

    # Make sure all values are arrays
    values = [np.atleast_1d(value) for value in dictionary.values()]

    for key in keys:
        table.add_column(key, justify="center")

    for i in range(len(values[0])):
        row = [
            (
                f"{value[i]:.4f}"
                if isinstance(value[i], float)
                else (
                    f"{(*value[i],)}"  # noqa: E231
                    if isinstance(value[i], list)
                    else f"{value[i]}"
                )
            )
            for value in values
        ]
        table.add_row(*row)

    if render:
        console.print(table, new_line_start=True)

    return table
