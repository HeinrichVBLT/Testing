# Testing
Test live clone, fetch, merge commands

# Modelling Job Breakdown
## [Refresh-declare-modelling-lifetime-bounds](https://github.com/blts-switzerland/notebooks-prod/blob/master/MODELLING-VIEWS/lifetime/refresh-declare-modelling-lifetime-bounds.ipynb)
**Summary**: Calculate whether a specific Channel_Type, Channel combination is active and how many times it has been outside of the churn window period

**Input Source/Dependency:** event.hevent

**Output Source:** modelling.lifetime_bounds
<br></br>

**Logical Breakdown:**

* Step 1: Import Input Source
* Step 2: Declare Churn_Window and set to 90 days
* Step 3: Measure difference (in seconds) between current timestamp and  previous timestamp of specific ChannelType, Channel, Timestamp combination
* Step 4: If difference between current timestamp and last timestamp > Churn_Window then set value to 1 else 0
* Step 5: Calculate running total based on how many times this ChannelType, Channel combination has been identified as outside the Churn_Window.
* Step 6: Only select records where Status=1, ChannelType is not 'Unknown', Channel is not NULL, and Channel does not start with '+27111'
* Step 7: Aggregate data set to a single ChannelType, Channel combination excluding channels that have >= 1 million transactions
* Step 8: If a ChannelType, Channel combination has a timestamp less than the Churn_Window it is seen as Active
* Step 9: Store results in data frame, test and persist to output source


<br></br>
## [Refresh-declare-modelling-lifetime-geolocation](https://github.com/blts-switzerland/notebooks-prod/blob/master/MODELLING-VIEWS/lifetime/refresh-declare-modelling-lifetime-geolocation.ipynb)
**Summary:** Assign Geolocations to Channel Type, Channel combinations for modelling purposes

**Input Source/Dependency:** event.hevent, md.v_postal_code_to_coordinates, modelling.lifetime

**Output Source:** modelling.lifetime_geolocation_postal_code, modelling.lifetime_geolocation_exact
<br></br>

**Logical Breakdown:**
 

**modelling.lifetime_geolocation_postal_code** (higher coverage but lower accuracy)
* Step 1: Calculate average latitude and longitude and combine with hevent and modelling.lifetime 
* Step 2: Only select records where3 Coname is 'AEON_SA' or 'TICKETPROS', and 'ADRESS_CODE' is not null
* Step 3: Repartition, test and persist to output source modelling.lifetime_geolocation_postal_code

**modelling.lifetime_geolocation_exact (lower coverage but better accuracy)**
* Step 1: Import Channel,ChannelType, Timestamp, Latitude and Longitude from hevent
* Step 2: Calculate average and sample std_dev for latitude and longitude, and combine with modelling.lifetime to bring in LIDX 
* Step 3: Calculate Number of locations as count of unique ChannelType, Channel combinations
* Step 4: Repartition, test and persist to output source modelling.lifetime_geolocation_postal_exact


<br></br>
## [Refresh-declare-modelling-lifetime](https://github.com/blts-switzerland/notebooks-prod/blob/master/MODELLING-VIEWS/lifetime/refresh-declare-modelling-lifetime.ipynb)

**Summary:** Create lifetime table containing a number of new features, and rebuild v_lifetime_cached

**Input Source/Dependency:** modelling.lifetime_bounds, event.hevent
**Output Source:** modelling.lifetime, modelling.v_lifetime_cached

**Logical Breakdown:**

* Step 1: Join lifetime bounds with hevent 

	* Where hevent.ChannelType <> 'UNKNOWN', hevent.Channel IS NOT null and HEVENT.Channel does not start with '+27111'
* Step 2: Create new features on Step 1 dataset for each unique ChannelType, Channel combination

	* New Features include: ORIGIN, PING, DAY_ORIGIN, DAY_PING, GENRES, CONAMES, VALUE_1, VALUE_0, NTRX_1, NTRX_0
* Step 3: Create TAU column , TAU: Date Difference between PING and ORIGIN.
* Step 4: Create Maturity column by running values through a custom function called c_maturity
* Step 5: Create Contactability by running values through a custom function called c_contactibility
* Step 6: Test and persist to output source
* Step 7: Rebuild view modelling.v_lifetime_cached


<br></br>
## [Refresh-declare-modelling-interest](https://github.com/blts-switzerland/notebooks-prod/blob/master/MODELLING-VIEWS/lifetime/refresh-declare-msisdn-interest.ipynb)
**Summary:** One hot encoding of GENRES column

**Input Source/Dependency:** modelling.lifetime

**Output Source:** modelling.msisdn_interest

**Logical Breakdown:**

* Step 1: Imports modelling.lifetime and extracts key columns where len(sqlfun.explode("GENRES")) < 32
* Step 2: Pivot/one hot encode GENRES for use in linear equations
* Step 3: Test and persist to output source


<br></br>
## [Refresh-declare-msisdn-networks](https://github.com/blts-switzerland/notebooks-prod/blob/master/MODELLING-VIEWS/lifetime/refresh-declare-msisdn-networks.ipynb)
**Summary:** Create dataset to show network changes

**Input Source/Dependency:** event.hevent, modelling.msisdn_networks

**Output Source:** modelling.v_msisdn_networks, modelling.msisdn_networks

**Logical Breakdown:**

* Step 1: Imports event.hevents and calculate TIMESTAMP, PREV_TIMESTAMP, NETWORK, PREV_NETWORK per record.
 
   * Functions Windowed: by CHANNELTYPE, CHANNEL
   * Where NETWORK is not null, CHANNELTYPE = 'MSISDN', YEAR > 2015 and((PREVIOUS_NETWORK <> NETWORK) or PREVIOUS_NETWORK is NULL) 
* Step 2: Test and persist to modelling.v_msisdn_networks
* Step 3: Import modelling.msisdn_networks, select latest MSISDN and create modelling.v_msisdn_latest_network


<br></br>
## [Refresh-declare-msisdn-spend](https://github.com/blts-switzerland/notebooks-prod/blob/master/MODELLING-VIEWS/lifetime/refresh-declare-msisdn-spend.ipynb)
**Summary:** Calculates spend metrics by company and net spend metrics for all companies

**Input Source/Dependency:** event.hevent, modelling.lifetime_bounds

**Output Source:** modelling.msisdn_spend_by_coname, modelling.msisdn_spend

**Function:** build_spend_metric_dataframe

**Logical Breakdown:**

* Step 1: Define function build_spend_metric_dataframe which accepts 2 dataframes(spend_transactions & spend_lifetime) to calculate spend

	* build_spend_metric_dataframe(pivotcols) = 'CONAME' to calculate spend by CONAME, = None to calculate spend for all companies
	* Function's purpose: Generates spend behavior from transactional times and values
* Step 2: Import subset of event.hevent to spend_transactions
	* Where YEAR >= 2015 and CONAME not in ('CELLFIND-ZA', 'CELLFIND-SB') and CHANNELTYPE='MSISDN' and STATUS > 0 and VALUE > 0
* Step 3: Import subset of modelling.lifetime_bounds to lifetimes where CHANNELTYPE='MSISDN'
* Step 4: Run build_spend_metric_dataframe function, with pivotcols set to 'CONAME'
* Step 5: Test and persist to modelling.msisdn_spend_by_coname
* Step 6: Run build_spend_metric_dataframe function, with pivotcols set to None
* Step 7: Join output of Step 4 and Step 6
* Step 8: Test and persist to modelling.msisdn_spend
