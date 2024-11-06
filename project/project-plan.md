# Project Plan

## COVID-19 Mortality In Selected Countries of The Americas
<!-- Give your project a short title. -->
A survey of COVID-19 mortality datasets from the United States, Chile and Brazil.

## Main Question

<!-- Think about one main question you want to answer based on the data. -->
1. How does Covid-19 mortality compare in the United States, Brazil and Chile in absolute and in relative numbers from 2019-2022.
<!-- Requires: -->
<!-- 1. US Covid Mortality -->
<!-- 2. US Total Population -->
<!-- 3. Brazil, Chile Covid Mortality -->
<!-- 5. Brazil, Chile Total Population -->
<!-- 6. For all above: Data from 2019-2022 (to be evaluated) -->

<!-- Assumptions: -->
<!-- 1. The mortality datasets are complete (Most certainly not the case: chaotic nature of pandemic, government inability in a crisis, difficulty of determining of whether someone died of covid or with covid, politics or the incentive for governments to keep reported numbers low as to avoid panic and to underscore the effectiveness of their pandemic measures and management.) -->
<!-- 2. Population data sets are up-to-date and complete (Difficulty of determining the exact population of a country (US f.e.))-->

2. How does Covid-19 mortality in hispanic countries compare to Covid-19 mortality in the hispanic US population
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

2. How did Covid-19 mortality change in the selected nations <!-- "cohorts" fancy --> from 2019-2022.
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

## Description

<!-- Describe your data science project in max. 200 words. Consider writing about why and how you attempt it. -->
The Covid-19 pandemic has had a lasting effect on the world. It has been an upheaval of economic, political and societal realities larger than most have seen in their lifetimes. This is not least due to the large number of lifes the virus has claimed.
In this project we analyze mortality resulting from the Corona Virus in the Americas. 

We do not perform a comprehensive analysis, rather we exemplify existing trends using open data datasets from a number of North and South American countries, that have been selected for certain quality criteria (such as completeness, accuracy, consistency and timeliness).
We aim to answer how COVID-19 mortality differs in relative terms between regions on the american continent. 
Similarly, we compare the progression of mortality in the time-span 2019-2022. Lastly, we explore the difference between COVID-19 mortality in hispanic/latin-american nations and the hispanic community in the United States of America.

The results can give insights into how deadly the pandemic has been in different regions and nations of the Americas. Additionally, they may give an idea of when COVID-19 mortality has peaked or slowed in South and North America.
As a result, this report can be a starting point for research into questions of how societal wealth and stability, government action or climate correlates to pandemic mortality.

## Datasources

<!-- Describe each datasources you plan to use in a section. Use the prefic "DatasourceX" where X is the id of the datasource. -->

### Datasource1: Covid-19 Mortality in Chile 
* Metadata URL: https://datos.gob.cl/dataset/defunciones-por-covid19
* Data URL: https://datos.gob.cl/dataset/8982a05a-91f7-422d-97bc-3eee08fde784/resource/8e5539b7-10b2-409b-ae5a-36dae4faf817/download/defunciones_covid19_2020_2024.csv
* Data Type: CSV
* License: Creative Commons Non-Commercial
<!-- CC-BY-NC is fine for our purposes according to the forum. -->

Short description of the DataSource.

### Datasource1: Name 
* Metadata URL: https://mobilithek.info/offers/-6901989592576801458
* Data URL: https://raw.githubusercontent.com/od-ms/radverkehr-zaehlstellen/main/100035541/2019-01.csv
* Data Type: CSV
* License: 

Short description of the DataSource.

### Datasource1: Name 
* Metadata URL: https://mobilithek.info/offers/-6901989592576801458
* Data URL: https://raw.githubusercontent.com/od-ms/radverkehr-zaehlstellen/main/100035541/2019-01.csv
* Data Type: CSV
* License:

Short description of the DataSource.

### Datasource1: World Population Prospects: Population Total
* Metadata URL: https://data.worldbank.org/indicator/SP.POP.TOTL?most_recent_year_desc=true
* Data URL: https://api.worldbank.org/v2/en/indicator/SP.POP.TOTL?downloadformat=csv
* Data Type: CSV
* License: Creative Commons Attribution 4.0 (CC-BY 4.0)

Short description of the DataSource.

## Work Packages

<!-- List of work packages ordered sequentially, each pointing to an issue with more details. -->

1. Example Issue [#1][i1]
2. ...

<!-- Finding Suitable Datasets (Estimate Relevancy, Accuracy, Consistency, Timeliness and Completeness)-->
<!-- Exploratory Data Analysis -->
<!-- Creating an automated data pipeline (Loading data & Data Transformation) -->
<!-- Extracting insights from data -->
<!-- Plotting the insights and data -->
<!-- Writing/Finalizing the report -->
[i1]: https://github.com/jvalue/made-template/issues/1
