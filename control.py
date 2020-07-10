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


def ingestControl(firstDate, lastDate, overwrite):

    """Need to check if control table exists - If it doesn't, create."""
    if not (path.exists("controlTable.csv")):
        """Control Table Creation"""
        dtypes = numpy.dtype([
            ('ImportDate', str),
            ('ImportStatus', str)
        ])

        data = numpy.empty(0, dtype=dtypes)
        controlTable = pd.DataFrame(data)

        """Write control table as .csv"""
        controlTable.to_csv("controlTable.csv", index=False, header=True)

    """Calculate dategap"""
    d1 = datetime.strptime(firstDate, "%Y%m%d")
    d2 = datetime.strptime(lastDate, "%Y%m%d")
    delta = d2 - d1
    dategap = []

    for i in range(delta.days +1):
        day = d1 + timedelta(days=i)
        dategap.append(day.strftime("%Y%m%d"))

    """Control table exists."""
    controlTable = pd.read_csv("controlTable.csv")
    """Need to cast dategap list to int to compare"""
    dategap = [int(i) for i in dategap]
    if len((set(controlTable["ImportDate"].tolist()) & set(dategap))) == 0 or overwrite:
        return True
    else:
        return False
