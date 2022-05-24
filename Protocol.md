# Alex's Protocol

One major issue with the current pipeline are the repetetive manual tasks needed in several of the processing steps. For example, in the past we had to manually use CardConvert to convert `tob1` binary files into `toa5` ASCII files for each download date and for each tower, meaning that there are hundreds of opportunities for mistakes. To fix this, we can just dump all the binary 10Hz files into one directory and do one pass-over with CardConvert.

## Raw File organization:

Files are split first by site, then by acquisition frequency. All "fast" files (10Hz) are kept in the same directory within a site, as well as all of the "slow" files ($\geq$1min). 

`'Bretfeld Mario/Chimney/Data/<site>/Fast'`
`'Bretfeld Mario/Chimney/Data/<site>/Slow'`

## CardConvert

Using cardconvert, we will convert all of the Fast files into ASCII format in one pass. The options below ensure that no data will be processed more than necessary.

Output files should look like `TOA5_<base filename>_<yyyy>_<mm>_<dd>_<hh><MM>_<n>.DAT`, where the timestamp represents the the FIRST line of the file. 

### Steps
1. Select Card Drive...: select the "Fast" directory for the site of interest (`./Data/<site>/Fast`.
2. Change Output Dir...: select the "Converted" directory within the `./EC Processing/<site>/Converted` 
3. Destination File Options...: 
	* File Format: ASCII Table Data (TOA5)
	* File Processing: 
		* Use time
	* File Naming:
		* TimeDate filenames
	* Create new filenames
	* TOA5-TOB1 Format:
		* Store record numbers
		* Store TimeStamp
	* Time Settings:
		* Start time: 01/01/2000 at 12:00:00 AM
		* Interval: 30min is recommended, but any reasonable multiple of 30min is also fine. 30min files are the most efficient from a processing perspective, but can be very unweildy when working with them manually. I don't recommend choosing an interval larger than 1 day, since that will really slow down the EddyPro processing. Note that if you're using a flux averaging window other than 30min (for example 1hr), then that will be the ideal interval size.
4. Start Conversion
5. If any empty files are encountered, click "okay" and continue. Write down the missing file to investigate afterwards.

You can expect CardConvert to take about 20 seconds to process a day's worth of data, depending on your drive speed or data connection.

## Standardized Converted Fast Files

Files that were incomplete for faulty before running CardConvert will still be incomplete after running CardConvert. This means that the data has to be manually cleaned. You can do this in the R script. In this case, the following can be seen
* the file timestamp may not land on a half-hour mark, indicating that the file is missing at least a minute of data at the start
* the file size will be less than expected
* the number of lines in the file is less than expected

The R script will open up each file, then

* add any missing timestamps
* populate any missing or invalid values with "NAN"
* standardize the header as follow:
	* Line 1: TOA5 header line containing the file format (TOA5), the datalogger ID and OS, the datalogger program, and some other information like the table name.
	* Line 2: Column headers, ordered as `Timestamp, Record, U, V, W, Ts, Diag_sonic, CO2, H2O, Pressure, Diag_IRGA`. If additional instruments are present, the following order is obeyed: `Timestamp, Record, Sonic_1, ..., Sonic_n, IRGA_1, ..., IRGA_n`
	* Line 3: Units, with wind velocity as m/s, temperature as deg C, CO2 density as mg m-3, and H20 density as g m-3
	* Line 4: type of record, usually "Smp" for "Sample"

Note that EddyPro doesn't actually care about header names, just the number of lines. This re-organization is for consistancy only.

"TOA5","9809","CR3000","9809","CR3000.Std.32.05","CPU:CPk_BBNF_EC17m_20190519.CR3","6475","TimeSeriesData"
"TIMESTAMP","RECORD","Ux_CSAT3_17m","Uy_CSAT3_17m","Uz_CSAT3_17m","Ts_CSAT3_17m","Ux_CSAT3_7m","Uy_CSAT3_7m","Uz_CSAT3_7m","Ts_CSAT3_7m","rho_c_LI7500","rho_v_LI7500","P_LI7500","DIAG_LI7500"
"TS","RN","m s-1","m s-1","m s-1","C","m s-1","m s-1","m s-1","C","mg m-3","g m-3","kPa","unitless"
"","","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp" 


