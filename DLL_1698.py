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

SNOWFLAKE_USER = data['snowflake_user']
SNOWFLAKE_PASSWORD = data['snowflake_password']
SNOWFLAKE_REGION = data['snowflake_region']
AUTHENTICATOR = data['authenticator']
SNOWFLAKE_DATABASE = data['snowflake_database']
SNOWFLAKE_WAREHOUSE = data['snowflake_warehouse']

f.close()


def get_Records():
    '''
    Checks the Snowflake database and check whether a new media type was added to aggregate
    '''
    try:

        try:
            ctx = snowflake.connector.connect(
                user= SNOWFLAKE_USER,
                password= SNOWFLAKE_PASSWORD,
                account = SNOWFLAKE_REGION,
                authenticator= AUTHENTICATOR,
                database = SNOWFLAKE_DATABASE,
                warehouse = SNOWFLAKE_WAREHOUSE
            )
            cs = ctx.cursor()

        except:
            return "Invalid SnowFlake Credentials"
    
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
        
        return 0;
    except:
        return 1;


def check_Latest_Data():
    try: #use yesterday as the basis since that is the most recent data
        try:
            ctx = snowflake.connector.connect(
                user= SNOWFLAKE_USER,
                password= SNOWFLAKE_PASSWORD,
                account = SNOWFLAKE_REGION,
                authenticator= AUTHENTICATOR,
                database = SNOWFLAKE_DATABASE,
                warehouse = SNOWFLAKE_WAREHOUSE
            )
        except:
            return "Invalid SnowFlake Credentials"

        cs = ctx.cursor()

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
        # tuple_array.append((300, 1)) #should recieve a email for this one
        # tuple_array.append((629, 212)) #shouldnt recieve a email and should be added to csv
        # tuple_array.append((300, 1)) #shouldnt recieve a email at all.

        df = pd.read_csv('pairs.csv', header=None)

        existing_pairs = set(tuple(row) for row in df.iloc[:, :2].values.tolist())
        existing_mediaID = set(df.iloc[:, 1].tolist())
        copy = set(existing_pairs)

        for values in tuple_array:
            if values not in existing_pairs:
                copy.add(values)

        with open('pairs.csv', mode='a') as filewriter:
            for values in copy:
                if values[1] not in existing_mediaID:
                    send_Email(values[0], values[1]) 
                    filewriter.write("{},{}".format(values[0], values[1]))
                    filewriter.write("\n") 
                elif values[1] in existing_mediaID and values not in existing_pairs:
                    filewriter.write("{},{}".format(values[0], values[1]))
                    filewriter.write("\n") 

    except: 
        return 0


def send_Email(Measurement_Source_ID, MediaType_ID):
    '''
    Method is used to send the email to the recipents.
    '''
    try:
        EMAIL_SUBJECT  = "CRITICAL: New Media Type ID Detected"

        msg = f"""From: {LOGIN_EMAIL}\r\nTo: {", ".join(RECIEVER_LIST)}\r\nSubject: {EMAIL_SUBJECT}\r\n\r\n"""
        msg += f"New Media Type ID Detected. \nMedia Type ID: {MediaType_ID}\nMeasurement Source ID: {Measurement_Source_ID}"

        server = smtplib.SMTP('smtp.gmail.com', 587)
        # server.set_debuglevel(1)
        server.ehlo()
        server.starttls()
        server.login(LOGIN_EMAIL, LOGIN_PASSWORD)
        server.sendmail(LOGIN_EMAIL, RECIEVER_LIST, msg)
        server.quit()
        return 0;
    except:
        return 1; 


if __name__ == "__main__":
    check_Latest_Data()