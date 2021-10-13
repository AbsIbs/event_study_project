# event_study_project
**INTRODUCTION**

The project is an investigation into the effects of the covid pandemic announcement by the WHO
on stock performance within the S & P 500 and FTSE 350. Specifcially, we investigate the Abnormal returns (AR);
this is the actual return of the stock minus the expected return (ER) by the index.

**METHODOLOGY**
- Historical price data was downloaded from yahoo finance. The data range is from 2019-01-01 to 2021-08-22
- There are 8 csv files in total which must be imported into MySQL
- The study will examine periods of (-20,+20), (-20,-1), (-1,+1) and (+1,+20)

**ANALYSIS**

**MySQL**
- A .sql file has been prepared which, when run, shall create the necessary database structure for the csvs to be imported into
- There are a total of 6 stored procedures. To use them, a user must simply type in their desired date and days before/after
- The stored procedures are split into 2 grouped. The grouped summary which calculates the Average Abnormal Return (AAR) per day per index
and the trading summary which calcualtes AR per day per ticker
- The stored procedures entitled "positive days" are to be used when the event period uses 2 dates AFTER the event day e.g. (+2,+20)
- The stored procedures entitled "negative days" are to be used when the event period uses 2 dates BEFORE the event day e.g. (-20,-2)

**TABLEAU**
- A summary dashboard has been created via tableau by exporting the relevant csv files from MySQL.The dashboards represent the data
visualisation aspect of the project.
- Dashboard https://public.tableau.com/views/EventStudy_16341434971040/120DASHBOARD?:language=en-GB&:display_count=n&:origin=viz_share_link
 
