import pandas as pd
from datetime import datetime, timedelta

"""Querying function
    Inputs: 
        bqClient: bigQuery Client
        parameterDate: Date used to query GA
    Output:
        Results: Pandas DataFrame
"""
def querying(bqClient, parameterFirstDate):

    controlTable = pd.read_csv("controlTable.csv")

    queryString = """
      SELECT
        date as date,
        visitid as visitid,
        fullvisitorid as fullvisitorid,
        CONCAT(fullVisitorId, CAST(visitStartTime AS STRING)) AS Sessions,
        hits.transaction.transactionId AS Transaction_ID,
        CASE
            WHEN totals.newVisits = 1 THEN 'New Visitor'
        ELSE
            'Returning Visitor'
        END AS user_type,
        geoNetwork.country AS country,
        geoNetwork.city AS city,
        device.deviceCategory AS device_category,
        CONCAT(trafficSource.source, '/', trafficSource.medium) AS source_medium,
        product.v2ProductName AS product_name,
        product.productSKU AS product_sku,
        product.productPrice AS product_price,
        product.productQuantity AS product_quantity,
        product.productRevenue AS product_revenue,
        totals.totalTransactionRevenue/1e6 AS revenue,
        IFNULL(totals.bounces, 0) AS bounce,
        CAST(hits.eCommerceAction.action_type AS INT64) AS action,
        CASE hits.eCommerceAction.action_type
          WHEN '0' THEN 'visit'
          WHEN '1' THEN 'product_list'
          WHEN '2' THEN 'product_detail'
          WHEN '3' THEN 'add_to_cart'
          WHEN '4' THEN 'remove_from_cart'
          WHEN '5' THEN 'checkout'
          WHEN '6' THEN 'order_complete'
          WHEN '7' THEN 'refund'
          WHEN '8' THEN 'checkout_options'
        END AS action_desc,
      FROM
        `bigquery-public-data.google_analytics_sample.ga_sessions_*`
        , UNNEST(hits) AS hits
        , UNNEST(hits.product) AS product
      WHERE
        _TABLE_SUFFIX = 'TO_REPLACE_BY_PARAMETER_FIRSTDATE'
    """.replace("TO_REPLACE_BY_PARAMETER_FIRSTDATE", parameterFirstDate)


    last_row = controlTable.size

    try:
        query_sessions = bqClient.query(queryString)
        results = pd.DataFrame(query_sessions.result().to_dataframe())
        if(results.size == 0):
            print("No new data.")
            controlTable.loc[last_row] = [parameterFirstDate, "OK", "NOK"]
            last_row += 1
            controlTable.to_csv("controlTable.csv", index=False)
            return False
        else:
            controlTable.loc[last_row] = [parameterFirstDate, "OK", "NOK"]
            last_row += 1
            controlTable.to_csv("controlTable.csv", index=False)
            return results
    except:
        print("Ups. Something went wrong")
        controlTable.loc[last_row] = [parameterFirstDate, "NOK", "NOK"]
        last_row+=1
        controlTable.to_csv("controlTable.csv", index=False)
        return False