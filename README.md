# DII-1698

As a Walled Garden developer,

I would like to compare the following data points in every day's data load using the data loaded into AGG_PARTNER_MEASURED_VIEWABILITY, AGG_PARTNER_MEASURED_VIEWABILITY_SITE, AGG_PARTNER_MEASURED_BRANDSAFETY & AGG_PARTNER_MEASURED_BRANDSAFETY_CREATIVE tables,

MEDIA_TYPE_ID
and the ID field MEDIA_TYPE_NEW table in Snowflake, so that we can detect any new Media Type ID that gets introduced in the aggregation side.

[TBD: Any other field needed to be included?]

We also need to send an alert email, if there are any new mediaTypeIDs detected to walledgardens-alerts@integralads.com email.

This will cover all S2S partners, as they migrate into the Pinterest pipeline.

Acceptance criteria:

Verify that we have a new task in the youtube pipeline to query the snowflake for MEDIA_TYPE_ID field the data loaded and compare it with the data found in MEDIA_TYPE_NEW.ID field. Please include the Measurement Source ID associated with facebook (Meas. Source ID = 4) in the alert.
Verify that this task is run after load, using the existing AWS post-load processor mechanism.
Verify that the email distribution goes to walledgardens-alerts@integralads.com for alerts
and the alert email contains the Measurement Source ID that introduced this new Media Type ID. (Note: Either this way, or link to the PD service directly)
