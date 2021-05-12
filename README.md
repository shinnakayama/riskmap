# Risk mapping

## codes
- `fishing_trips.sql`: query for fishing trips in GFW datasets
- `transshipment_trips.sql`: query for trips by carrier vessels in GFW datasets
- `transshipment_loitering.sql`: query for trips by carrier vessels with loitering in GFW datasets
- `port_stop_duration.sql`: query for port stop duration in GFW datasets
- `port_visit.sql`: query for port visit for PSMA analysis

- `at_sea_analysis.py`: XGBoost and SHAP analysis for risk of fishing trips
- `transshipment_analysis.py`: XGBoost and SHAP analysis for risk of trips by carrier vessels
- `analyze_port_stop_duration.r`: linear mixed model on port stop duration by flag groups / gear type 
- `baci_analysis.py`: PSMA analysis


## data
- `port_risk.csv`: port risk assessment
- `c188.csv`: nations with C188
- `GDP.csv`: GDP by nations
- `HDI.csv`: HDI by nations
- `pew_port_capacity.csv`: Port capacity collated by PEW
- `Updated_PSMA_dates_27 JAN 2021.csv`: list of PSMA parties and dates 
