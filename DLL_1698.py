import smtplib
import json
import snowflake.connector
import pendulum
import pandas as pd

TODAY = pendulum.today()
YESTERDAY = TODAY.subtract(days = 1) #
f = open('snowflake_credentials.json')
data = json.load(f)

LOGIN_EMAIL = data['gmail_address']
LOGIN_PASSWORD = data['gmail_2fa_password']
RECIEVER_LIST = ['ilin@integralads.com', 'ivanlin0418@gmail.com']


def get_Records():
    '''
    Checks the Snowflake database and check whether a new media type was added to aggregate
    '''

    f = open('snowflake_credentials.json')
    data = json.load(f)

    ctx = snowflake.connector.connect(
        user= data['snowflake_user'],
        password= data['snowflake_password'],
        account = data['snowflake_region'],
        authenticator= data['authenticator'],
        database = data['snowflake_database'],
        warehouse = data['snowflake_warehouse']
    )

    f.close()

    cs = ctx.cursor()

    cs.execute(
    """
    SELECT 
        DISTINCT
        MEASUREMENT_SOURCE_ID, 
        MEDIA_TYPE_ID
    FROM 
        CDS_PROD.ANALYTICS.AGG_PARTNER_MEASURED_VIEWABILITY
    WHERE 
        HIT_DATE < %s
    UNION 
    SELECT DISTINCT
        MEASUREMENT_SOURCE_ID, 
        MEDIA_TYPE_ID
    FROM 
        CDS_PROD.ANALYTICS.AGG_PARTNER_MEASURED_VIEWABILITY_SITE
    WHERE 
        HIT_DATE < %s
    UNION 
    SELECT DISTINCT
        MEASUREMENT_SOURCE_ID, 
        MEDIA_TYPE_ID
    FROM 
        CDS_PROD.ANALYTICS.AGG_PARTNER_MEASURED_BRANDSAFETY
    WHERE 
        HIT_DATE < %s
    UNION 
    SELECT DISTINCT
        MEASUREMENT_SOURCE_ID, 
        MEDIA_TYPE_ID
    FROM 
        CDS_PROD.ANALYTICS.AGG_PARTNER_MEASURED_BRANDSAFETY_CREATIVE
    WHERE 
        HIT_DATE < %s
    """, (str(YESTERDAY)[:10], str(YESTERDAY)[:10], str(YESTERDAY)[:10], str(YESTERDAY)[:10]))

    cs.fetch_pandas_all().to_csv("pairs.csv", sep=",", header=False, index=False)



def check_Data():

    TODAY = pendulum.today()
    YESTERDAY = TODAY.subtract(days = 1) #

    f = open('snowflake_credentials.json')
    data = json.load(f)

    ctx = snowflake.connector.connect(
        user= data['snowflake_user'],
        password= data['snowflake_password'],
        account = data['snowflake_region'],
        authenticator= data['authenticator'],
        database = data['snowflake_database'],
        warehouse = data['snowflake_warehouse']
    )
    cs = ctx.cursor()
    f.close()
    cs.execute(
    """
    SELECT DISTINCT
        MEASUREMENT_SOURCE_ID, 
        MEDIA_TYPE_ID
    FROM 
        CDS_PROD.ANALYTICS.AGG_PARTNER_MEASURED_VIEWABILITY
    WHERE 
        HIT_DATE = %s
    UNION 
    SELECT DISTINCT
        MEASUREMENT_SOURCE_ID, 
        MEDIA_TYPE_ID
    FROM 
        CDS_PROD.ANALYTICS.AGG_PARTNER_MEASURED_VIEWABILITY_SITE
    WHERE 
        HIT_DATE = %s
    UNION 
    SELECT DISTINCT
        MEASUREMENT_SOURCE_ID, 
        MEDIA_TYPE_ID
    FROM 
        CDS_PROD.ANALYTICS.AGG_PARTNER_MEASURED_BRANDSAFETY
    WHERE 
        HIT_DATE = %s
    UNION 
    SELECT DISTINCT
        MEASUREMENT_SOURCE_ID, 
        MEDIA_TYPE_ID
    FROM 
        CDS_PROD.ANALYTICS.AGG_PARTNER_MEASURED_BRANDSAFETY_CREATIVE
    WHERE 
        HIT_DATE = %s
    """, (str(YESTERDAY)[:10], str(YESTERDAY)[:10], str(YESTERDAY)[:10], str(YESTERDAY)[:10],))

    tuple_array = [tuple(x) for x in cs.fetch_pandas_all().to_records(index=False)]
    tuple_array.append((300, 1))
    df = pd.read_csv('pairs.csv', header=None)
    lst = set(df.iloc[:, 1].tolist())

    for values in tuple_array:
        if values[1] not in lst:
            print (values[0], values[1])   
            send_Email(values[0], values[1]) 

        
def send_Email(Measurement_Source_ID, MediaType_ID ):
    EMAIL_SUBJECT  = "Alert: New Media Type ID Detected"

    msg = f"""From: {LOGIN_EMAIL}\r\nTo: {", ".join(RECIEVER_LIST)}\r\nSubject: {EMAIL_SUBJECT}\r\n\r\n"""
    msg += f"New Media Type ID Detected. \nMedia Type ID: {MediaType_ID}\nMeasurement Source ID: {Measurement_Source_ID}"

    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.set_debuglevel(1)
    server.ehlo()
    server.starttls()
    server.login(LOGIN_EMAIL, LOGIN_PASSWORD)
    server.sendmail(LOGIN_EMAIL, RECIEVER_LIST, msg)
    server.quit()

if __name__ == "__main__":
    check_Data()