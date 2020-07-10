import numpy
import pandas as pd
from datetime import datetime, timedelta
from os import path

"""ValidateDate Function
    Input:
        dateToImport
    Output:
        True if valid
"""

def validateDate(dateToImport):
    try:
        datetime.strptime(dateToImport, "%Y%m%d")
        return True
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYYmmDD. As, for example: 20170401")


"""ingestControl Function
    Input:
        dateToImport, overwrite
    Output:
        True/False
"""


def ingestControl(firstDate, overwrite):

    """Need to check if control table exists - If it doesn't, create."""
    if not (path.exists("controlTable.csv")):
        """Control Table Creation"""
        dtypes = numpy.dtype([
            ('ImportDate', str),
            ('ImportStatus', str),
            ('ParquetWritten', str)
        ])

        data = numpy.empty(0, dtype=dtypes)
        controlTable = pd.DataFrame(data)

        """Write control table as .csv"""
        controlTable.to_csv("controlTable.csv", index=False, header=True)


    """Control table exists."""
    controlTable = pd.read_csv("controlTable.csv")

    if controlTable[controlTable["ImportDate"] == firstDate].size == 0 or overwrite:
        return True
    else:
        return False
