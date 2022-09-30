{
	"metadata": {
		"kernelspec": {
			"name": "glue_pyspark",
			"display_name": "Glue PySpark",
			"language": "python"
		},
		"language_info": {
			"name": "Python_Glue_Session",
			"mimetype": "text/x-python",
			"codemirror_mode": {
				"name": "python",
				"version": 3
			},
			"pygments_lexer": "python3",
			"file_extension": ".py"
		},
		"vscode": {
			"interpreter": {
				"hash": "d11a57ad427e0fb59d95b3e8266b4d202c8cc54300c7c21682f35055b9b2af29"
			}
		}
	},
	"nbformat_minor": 4,
	"nbformat": 4,
	"cells": [
		{
			"cell_type": "code",
			"source": "import numpy as mp\nimport pandas as pd\n",
			"metadata": {
				"trusted": true
			},
			"execution_count": null,
			"outputs": [
				{
					"name": "stdout",
					"text": "Welcome to the Glue Interactive Sessions Kernel\nFor more information on available magic commands, please type %help in any new cell.\n\nPlease view our Getting Started page to access the most up-to-date information on the Interactive Sessions kernel: https://docs.aws.amazon.com/glue/latest/dg/interactive-sessions.html\nIt looks like there is a newer version of the kernel available. The latest version is 0.35 and you have 0.30 installed.\nPlease run `pip install --upgrade aws-glue-sessions` to upgrade your kernel\nAuthenticating with environment variables and user-defined glue_role_arn: arn:aws:iam::816277082181:role/LabRole\nAttempting to use existing AssumeRole session credentials.\nTrying to create a Glue session for the kernel.\nWorker Type: G.1X\nNumber of Workers: 5\nSession ID: e28dbcbf-774f-41fd-942b-b9205a065c92\nApplying the following default arguments:\n--glue_kernel_version 0.30\n--enable-glue-datacatalog true\nWaiting for session e28dbcbf-774f-41fd-942b-b9205a065c92 to get into ready status...\nSession e28dbcbf-774f-41fd-942b-b9205a065c92 has been created\n\n\n",
					"output_type": "stream"
				}
			]
		},
		{
			"cell_type": "code",
			"source": "df=pd.read_csv(\"s3://devops-24/US_Accidents_Dec21_updated.csv\")",
			"metadata": {
				"trusted": true
			},
			"execution_count": 1,
			"outputs": [
				{
					"name": "stdout",
					"text": "\n",
					"output_type": "stream"
				}
			]
		},
		{
			"cell_type": "code",
			"source": "df['Start_Time'] = pd.to_datetime(df['Start_Time'])\ndf['End_Time'] = pd.to_datetime(df['End_Time'])\n#add column representing hour of the start of the accident\ndf['Day'] = (df['Start_Time']).dt.weekday\ndf.head()",
			"metadata": {
				"trusted": true
			},
			"execution_count": 2,
			"outputs": [
				{
					"name": "stdout",
					"text": "    ID  Severity  ... Astronomical_Twilight Day\n0  A-1         3  ...                 Night   0\n1  A-2         2  ...                 Night   0\n2  A-3         2  ...                   Day   0\n3  A-4         2  ...                   Day   0\n4  A-5         3  ...                   Day   0\n\n[5 rows x 48 columns]\n",
					"output_type": "stream"
				}
			]
		},
		{
			"cell_type": "code",
			"source": "df['time'] = pd.to_datetime(df.Start_Time, format='%Y-%m-%d %H:%M:%S')\n#df = df.set_index('time')\ndf.head()",
			"metadata": {
				"trusted": true
			},
			"execution_count": 3,
			"outputs": [
				{
					"name": "stdout",
					"text": "    ID  Severity  ... Day                time\n0  A-1         3  ...   0 2016-02-08 00:37:08\n1  A-2         2  ...   0 2016-02-08 05:56:20\n2  A-3         2  ...   0 2016-02-08 06:15:39\n3  A-4         2  ...   0 2016-02-08 06:51:45\n4  A-5         3  ...   0 2016-02-08 07:53:43\n\n[5 rows x 49 columns]\n",
					"output_type": "stream"
				}
			]
		},
		{
			"cell_type": "code",
			"source": "df1 = df.drop(columns = ['Start_Time','End_Time','End_Lat','End_Lng','Description','Number','Street','Side','Zipcode','Timezone','Country','Airport_Code','Weather_Timestamp','Nautical_Twilight','Astronomical_Twilight','Civil_Twilight','City']);\n",
			"metadata": {
				"trusted": true
			},
			"execution_count": 4,
			"outputs": [
				{
					"name": "stdout",
					"text": "\n",
					"output_type": "stream"
				}
			]
		},
		{
			"cell_type": "code",
			"source": "df1.isnull().sum()",
			"metadata": {
				"trusted": true
			},
			"execution_count": 5,
			"outputs": [
				{
					"name": "stdout",
					"text": "ID                        0\nSeverity                  0\nStart_Lat                 0\nStart_Lng                 0\nDistance(mi)              0\nCounty                    0\nState                     0\nTemperature(F)        69274\nWind_Chill(F)        469643\nHumidity(%)           73092\nPressure(in)          59200\nVisibility(mi)        70546\nWind_Direction        73775\nWind_Speed(mph)      157944\nPrecipitation(in)    549458\nWeather_Condition     70636\nAmenity                   0\nBump                      0\nCrossing                  0\nGive_Way                  0\nJunction                  0\nNo_Exit                   0\nRailway                   0\nRoundabout                0\nStation                   0\nStop                      0\nTraffic_Calming           0\nTraffic_Signal            0\nTurning_Loop              0\nSunrise_Sunset         2867\nDay                       0\ntime                      0\ndtype: int64\n",
					"output_type": "stream"
				}
			]
		},
		{
			"cell_type": "code",
			"source": "pmean = df1['Pressure(in)'].mean()\ntmean = df1['Temperature(F)'].mean()\nwcmean = df1['Wind_Chill(F)'].mean()\nhmean = df1['Humidity(%)'].mean()\nwsmean = df1['Wind_Speed(mph)'].mean()\nprmean = df1['Precipitation(in)'].mean()",
			"metadata": {
				"trusted": true
			},
			"execution_count": 6,
			"outputs": [
				{
					"name": "stdout",
					"text": "\n",
					"output_type": "stream"
				}
			]
		},
		{
			"cell_type": "code",
			"source": "df1['Pressure(in)']=df1['Pressure(in)'].fillna(pmean)\ndf1['Temperature(F)'] = df1['Temperature(F)'].fillna(tmean)\ndf1['Wind_Chill(F)'] = df1['Wind_Chill(F)'].fillna(wcmean)\ndf1['Humidity(%)'] = df1['Humidity(%)'].fillna(hmean)\ndf1['Wind_Speed(mph)'] = df1['Wind_Speed(mph)'].fillna(wsmean)\ndf1['Precipitation(in)']=df1['Precipitation(in)'].fillna(prmean)",
			"metadata": {
				"trusted": true
			},
			"execution_count": 7,
			"outputs": [
				{
					"name": "stdout",
					"text": "\n",
					"output_type": "stream"
				}
			]
		},
		{
			"cell_type": "code",
			"source": "df1.isnull().sum()",
			"metadata": {
				"trusted": true
			},
			"execution_count": 8,
			"outputs": [
				{
					"name": "stdout",
					"text": "ID                       0\nSeverity                 0\nStart_Lat                0\nStart_Lng                0\nDistance(mi)             0\nCounty                   0\nState                    0\nTemperature(F)           0\nWind_Chill(F)            0\nHumidity(%)              0\nPressure(in)             0\nVisibility(mi)       70546\nWind_Direction       73775\nWind_Speed(mph)          0\nPrecipitation(in)        0\nWeather_Condition    70636\nAmenity                  0\nBump                     0\nCrossing                 0\nGive_Way                 0\nJunction                 0\nNo_Exit                  0\nRailway                  0\nRoundabout               0\nStation                  0\nStop                     0\nTraffic_Calming          0\nTraffic_Signal           0\nTurning_Loop             0\nSunrise_Sunset        2867\nDay                      0\ntime                     0\ndtype: int64\n",
					"output_type": "stream"
				}
			]
		},
		{
			"cell_type": "code",
			"source": "visMode = df1[\"Visibility(mi)\"].mode()\ndf1['Visibility(mi)'] = df1['Visibility(mi)'].fillna(df1['Visibility(mi)'].mode()[0])\ndf1['Wind_Direction'] = df1['Wind_Direction'].fillna(df1['Wind_Direction'].mode()[0])\ndf1['Weather_Condition'] = df1['Weather_Condition'].fillna(df1['Weather_Condition'].mode()[0])\ndf1['Sunrise_Sunset'] = df1['Sunrise_Sunset'].fillna(df1['Sunrise_Sunset'].mode()[0])\n",
			"metadata": {
				"trusted": true
			},
			"execution_count": 9,
			"outputs": [
				{
					"name": "stdout",
					"text": "\n",
					"output_type": "stream"
				}
			]
		},
		{
			"cell_type": "code",
			"source": "df1.isna().sum()",
			"metadata": {
				"trusted": true
			},
			"execution_count": 10,
			"outputs": [
				{
					"name": "stdout",
					"text": "ID                   0\nSeverity             0\nStart_Lat            0\nStart_Lng            0\nDistance(mi)         0\nCounty               0\nState                0\nTemperature(F)       0\nWind_Chill(F)        0\nHumidity(%)          0\nPressure(in)         0\nVisibility(mi)       0\nWind_Direction       0\nWind_Speed(mph)      0\nPrecipitation(in)    0\nWeather_Condition    0\nAmenity              0\nBump                 0\nCrossing             0\nGive_Way             0\nJunction             0\nNo_Exit              0\nRailway              0\nRoundabout           0\nStation              0\nStop                 0\nTraffic_Calming      0\nTraffic_Signal       0\nTurning_Loop         0\nSunrise_Sunset       0\nDay                  0\ntime                 0\ndtype: int64\n",
					"output_type": "stream"
				}
			]
		},
		{
			"cell_type": "code",
			"source": "df1.rename(columns = {'Temperature(F)':'Temperature','Wind_Chill(F)':'Wind_Chill',\n'Pressure(in)':'Pressure','Visibility(mi)':'Visibility','Wind_Speed(mph)':'Wind_Speed','Precipitation(in)':'Precipitation','Humidity(%)':'Humidity'}, inplace = True)\n\n",
			"metadata": {
				"trusted": true
			},
			"execution_count": 11,
			"outputs": [
				{
					"name": "stdout",
					"text": "\n",
					"output_type": "stream"
				}
			]
		},
		{
			"cell_type": "code",
			"source": "df1.rename(columns = {'Distance(mi)':'Distance'},inplace=True)",
			"metadata": {
				"trusted": true
			},
			"execution_count": 12,
			"outputs": [
				{
					"name": "stdout",
					"text": "\n",
					"output_type": "stream"
				}
			]
		},
		{
			"cell_type": "code",
			"source": "df1.describe()",
			"metadata": {
				"trusted": true
			},
			"execution_count": 13,
			"outputs": [
				{
					"name": "stdout",
					"text": "           Severity     Start_Lat  ...  Precipitation           Day\ncount  2.845342e+06  2.845342e+06  ...   2.845342e+06  2.845342e+06\nmean   2.137572e+00  3.624520e+01  ...   7.016940e-03  2.750765e+00\nstd    4.787216e-01  5.363797e+00  ...   8.397790e-02  1.865765e+00\nmin    1.000000e+00  2.456603e+01  ...   0.000000e+00  0.000000e+00\n25%    2.000000e+00  3.344517e+01  ...   0.000000e+00  1.000000e+00\n50%    2.000000e+00  3.609861e+01  ...   0.000000e+00  3.000000e+00\n75%    2.000000e+00  4.016024e+01  ...   7.016940e-03  4.000000e+00\nmax    4.000000e+00  4.900058e+01  ...   2.400000e+01  6.000000e+00\n\n[8 rows x 12 columns]\n",
					"output_type": "stream"
				}
			]
		},
		{
			"cell_type": "code",
			"source": "df1.info()",
			"metadata": {
				"trusted": true
			},
			"execution_count": 14,
			"outputs": [
				{
					"name": "stdout",
					"text": "<class 'pandas.core.frame.DataFrame'>\nRangeIndex: 2845342 entries, 0 to 2845341\nData columns (total 32 columns):\n #   Column             Dtype         \n---  ------             -----         \n 0   ID                 object        \n 1   Severity           int64         \n 2   Start_Lat          float64       \n 3   Start_Lng          float64       \n 4   Distance           float64       \n 5   County             object        \n 6   State              object        \n 7   Temperature        float64       \n 8   Wind_Chill         float64       \n 9   Humidity           float64       \n 10  Pressure           float64       \n 11  Visibility         float64       \n 12  Wind_Direction     object        \n 13  Wind_Speed         float64       \n 14  Precipitation      float64       \n 15  Weather_Condition  object        \n 16  Amenity            bool          \n 17  Bump               bool          \n 18  Crossing           bool          \n 19  Give_Way           bool          \n 20  Junction           bool          \n 21  No_Exit            bool          \n 22  Railway            bool          \n 23  Roundabout         bool          \n 24  Station            bool          \n 25  Stop               bool          \n 26  Traffic_Calming    bool          \n 27  Traffic_Signal     bool          \n 28  Turning_Loop       bool          \n 29  Sunrise_Sunset     object        \n 30  Day                int64         \n 31  time               datetime64[ns]\ndtypes: bool(13), datetime64[ns](1), float64(10), int64(2), object(6)\nmemory usage: 447.7+ MB\n",
					"output_type": "stream"
				}
			]
		},
		{
			"cell_type": "code",
			"source": "df1.to_csv(\"s3://promexa/Us_accident_visualization.csv\",index=False)",
			"metadata": {
				"trusted": true
			},
			"execution_count": 16,
			"outputs": [
				{
					"name": "stdout",
					"text": "\n",
					"output_type": "stream"
				}
			]
		},
		{
			"cell_type": "code",
			"source": "",
			"metadata": {},
			"execution_count": null,
			"outputs": []
		}
	]
}