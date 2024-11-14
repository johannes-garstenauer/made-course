# Project Plan

## COVID-19 Mortality In Selected Countries of The Americas

A survey of COVID-19 mortality datasets from the United States, Chile, Colombia and Mexico (subject to change).

## Main Question

1. COVID-19 Mortality in the Americas: How does Covid-19 mortality compare in the selected nations in South- and North America in absolute and in relative terms from 2020 to 2023?
<!-- Requires: -->
<!-- 1. US Covid Mortality -->
<!-- 2. US Total Population -->
<!-- 3. SA Covid Mortality -->
<!-- 5. SA Total Population -->
<!-- 6. For all above: Data from 2019-2023 (to be evaluated) -->

<!-- Assumptions: -->
<!-- 1. The mortality datasets are complete (Most certainly not the case: chaotic nature of pandemic, government inability in a crisis, difficulty of determining of whether someone died of covid or with covid, politics or the incentive for governments to keep reported numbers low as to avoid panic and to underscore the effectiveness of their pandemic measures and management.) -->
<!-- 2. Population data sets are up-to-date and complete (Difficulty of determining the exact population of a country (US f.e.))-->

2. How did COVID-19 mortality develop in the selected nations from 2020 to 2023?
<!-- Requires: -->
<!-- 1. US Covid Mortality -->
<!-- 2. US Covid Mortality of Hispanics -->
<!-- 3. US total population and hispanic population-->
<!-- 4. Brazil, Chile Covid Mortality -->
<!-- 5. Brazil, Chile Total Population -->
<!-- 6. For all above: Data from 2019-2022-->

<!-- Assumptions -->
<!-- 1. South American Countries are 100% hispanic -->
<!-- 2. Mortality Datasets are complete -->
<!-- 3. Population data sets are accurate -->

3. How does COVID-19 mortality in hispanic countries compare to COVID-19 mortality in the hispanic US population from 2020 to 2023?
<!-- Requires: -->
<!-- 1. US Covid Mortality -->
<!-- 3. Brazil, Chile Covid Mortality -->
<!-- 6. For all above: Data from 2019-2022 (to be evaluated) -->

<!-- Assumptions -->
<!-- 1. Mortality Datasets are complete -->
<!-- 2. The dataset accuracy remains consistent over time (Probably, in the beginning of the pandemic there is less accurate data due to entropy of an unanticipated crisis and towards the end due to lessening of interest in the data -->


<!-- Possible Conclusions:-->
<!-- 1. Impact of wealth inequality and societal stability on health outcome (Further research would be necessary, for example by taking apart mortality by wealth and societal status) -->
<!-- 2. Impact of climate on health outcomes (Further research would be necessary to establish a correlation or causation) -->
<!-- 3. Impact of pandemic induced government measures on Covid mortality (Further research could look at the correlations between government actions like lockdowns, whether they correlate to spikes or drops in timeline and their effectiveness -->
<!-- 4. Seasonal patterns in Covid Mortality -->

<!-- Challenges -->
<!-- 1. Foreign Language Datasets -->
<!-- 2. A large number of datasets in different formats, time spans, accuracy and completeness, from various sources and under various licenses -->

## Description

The COVID-19 pandemic has had a lasting effect on the world. It has been an upheaval of economic, political and societal realities larger than most have ever seen in their lifetimes. This is not least due to the large number of lifes the pandemic has claimed.
In this project we analyze mortality resulting from the Corona Virus in the Americas. 

We do not perform a comprehensive analysis, rather we exemplify existing trends using open data datasets from a number of North and South American countries, that have been selected for certain quality criteria (such as completeness, accuracy, consistency and timeliness).
We aim to answer how COVID-19 mortality differs in relative terms between nations on the american continent. 
Similarly, we compare the progression of mortality in the time-span 2020-2023. Lastly, we explore the difference between COVID-19 mortality in hispanic/latin-american nations and the hispanic community in the United States of America.

The results can give insights into how deadly the pandemic has been in different regions and nations of the Americas. Additionally, they may give an idea of when COVID-19 mortality has peaked or slowed in South and North America as well as how it has affected the hispanic community in particular.
As a result, this report can be a starting point for research into questions of how societal wealth and stability, government action or climate conditions correlate to pandemic mortality.

## Datasources

### Datasource 1: (Chile) Defunciones por COVID19
:warning:It appears like this open data portal is currently *offline*:warning:

* Metadata URL: https://datos.gob.cl/dataset/defunciones-por-covid19
* Data URL: https://datos.gob.cl/dataset/8982a05a-91f7-422d-97bc-3eee08fde784/resource/8e5539b7-10b2-409b-ae5a-36dae4faf817/download/defunciones_covid19_2020_2024.csv
* Data Type: CSV
* License: Creative Commons Non-Commercial

Total number of COVID-19 deaths in Chile from 1/2020 to 9/2024 on a daily/case-by-case basis. This dataset is provided by the official government data portal under the Creative Commons NonCommercial license.

### Datasource 2: (USA) Monthly COVID-19 Death Rates per 100,000 Population by Age Group, Race and Ethnicity, Sex, and Region with Double Stratification
* Metadata URL: https://data.cdc.gov/Public-Health-Surveillance/Monthly-COVID-19-Death-Rates-per-100-000-Populatio/exs3-hbne/about_data
* Data URL: https://data.cdc.gov/api/views/exs3-hbne/rows.csv?fourfour=exs3-hbne&cacheBust=1729520760&date=20241106&accessType=DOWNLOAD
* Data Type: CSV
* License: Public Domain U.S. Government

Total number of COVID-19 deaths in the USA from 1/2020 to 9/2024 on a monthly basis. Notably, they are grouped by certain characeristics, such as 'Race'. This dataset is provided by the official government agency 'Center for disease control's data portal under Public Domain of the U.S. Government.

### Datasource 3: (Colombia) Fallecidos COVID en Colombia
* Metadata URL: https://www.datos.gov.co/en/Salud-y-Protecci-n-Social/Fallecidos-COVID-en-Colombia/jp5m-e7yr/about_data
* Data URL: https://www.datos.gov.co/api/views/jp5m-e7yr/rows.csv?fourfour=jp5m-e7yr&cacheBust=1705599009&date=20241106&accessType=DOWNLOAD
* Data Type: CSV
* License: CC BY-SA 4.0: Attribution-ShareAlike 4.0 International

Total number of COVID-19 deaths in Colombia from 3/2020 to 1/2024 on a daily/case-by-case basis. This dataset is a view of a dataset the official government data portal under the Creative Commons Attribution-ShareAlike 4.0 International license.

### Datasource 4: (Mexico) Casos Diarios Estado Nacional Defunciones
* Metadata URL: https://datos.covid-19.conacyt.mx/
* Data URL: https://datos.covid-19.conacyt.mx/Downloads/Files/Casos_Diarios_Estado_Nacional_Defunciones_20230625.csv
* Data Type: CSV
* License: libre uso MX de los Datos Abiertos del Gobierno de México

Total number of COVID-19 deaths in Mexico from 3/2020 to 6/2023 on a daily basis. This dataset is provided by the official government open data portal licensed under the 'libre uso MX de los Datos Abiertos del Gobierno de México' terms.

### Datasource 5: World Population Prospects: Population Total
* Metadata URL: https://data.worldbank.org/indicator/SP.POP.TOTL?most_recent_year_desc=true
* Data URL: https://api.worldbank.org/v2/en/indicator/SP.POP.TOTL?downloadformat=csv
* Data Type: CSV
* License: Creative Commons Attribution 4.0 (CC-BY 4.0)

World population numbers for each nation and year from 1960-2023. This data is employed for calculating the mortality relative to total population. This dataset is provided by the World Bank Group (WBG), the worldl's largest development loan bank and an observer at the United Nations. Its open data portal content is licensed under Creative Commons Attribution 4.0 (CC-BY 4.0).

## Work Packages

1. Finding Suitable Datasets of COVID-19 Mortality in South- and North American Nations [#1][i1]
2. Exploratory Data Analysis [#2][i2]
3. Creating An Automated Data Pipeline [#3][i3]
4. Extracting Insights From Data [#4][i4]
5. Plotting Of Insights and Data [#5][i5]
5. Writing And Finalizing The Report [#6][i6]

[i1]: https://github.com/johannes-garstenauer/made-course/issues/2
[i2]: https://github.com/johannes-garstenauer/made-course/issues/3
[i3]: https://github.com/johannes-garstenauer/made-course/issues/4
[i4]: https://github.com/johannes-garstenauer/made-course/issues/5
[i5]: https://github.com/johannes-garstenauer/made-course/issues/6
[i6]: https://github.com/johannes-garstenauer/made-course/issues/7
