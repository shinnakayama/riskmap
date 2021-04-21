import os
import sys
import numpy as np
import pandas as pd
import country_converter as coco
import pymc3 as pm
import arviz as az
import patsy
import theano.tensor as tt


#----------------------------
# load PSMA country data
country = pd.read_csv('Updated_PSMA_dates_27 JAN 2021.csv')
country.dropna(axis=0, how='all', inplace=True)
country = country[country.Country != 'European Union'].copy()
country = country[country.Entry_into_force_date.notnull()].copy()
country['Entry_into_force_date'] = pd.to_datetime(country.Entry_into_force_date)

country['iso3'] = [coco.convert(names=x, to='ISO3') for x in country.Country]
in2016 = [(x >= pd.Timestamp(2016, 1, 1)) * (x < pd.Timestamp(2017, 1, 1)) for x in country.Entry_into_force_date]
in2017 = [(x >= pd.Timestamp(2017, 1, 1)) * (x < pd.Timestamp(2018, 1, 1)) for x in country.Entry_into_force_date]


# list of countries with PSMA in 2016
psma_party2016 = country.loc[np.array(in2016)==1, 'iso3']
# list of countries with PSMA in 2017
psma_party2017 = country.loc[np.array(in2017)==1, 'iso3']


#-----------------------------
# output of `port_visit.sql`
data = pd.read_csv('port_visit.csv')


# remove countries that ratified PSMA in 2017
foo = data[~data.port_iso3.isin(psma_party2017)].copy()


foo = foo[foo.flag.notnull()]
foo = foo[foo.port_iso3.notnull()]
foo = foo[foo.vessel_class.notnull()]


# territories and their sovereign nations
# queried from GFW on March 29, 2021
# `world-fishing-827.gfw_research.eez_info
eez = pd.read_csv('eez_info.csv')
eez = eez[eez.eez_type=='200NM']
eez = eez[eez.territory1_iso3 != eez.sovereign1_iso3]
pair = eez[['territory1_iso3', 'sovereign1_iso3']].drop_duplicates()
# add Hong Kong and Macau
pair = pair.append({'territory1_iso3':'MAC', 'sovereign1_iso3':'CHN'}, ignore_index=True)
pair = pair.append({'territory1_iso3':'HKG', 'sovereign1_iso3':'CHN'}, ignore_index=True)


# add sovereign nations to their territories for port_iso3 and flag
foo['flag_sovereign'] = foo['flag']
foo['port_iso3_sovereign'] = foo['port_iso3']

for i,x in enumerate(pair.territory1_iso3):
    foo.loc[foo.flag==x, 'flag_sovereign'] = pair.sovereign1_iso3[i]
    foo.loc[foo.port_iso3==x, 'port_iso3_sovereign'] = pair.sovereign1_iso3[i]


# foreign vessels
bar = foo[foo.port_iso3_sovereign != foo.flag_sovereign].copy()


# variable of interest
fishing_gear = ['trollers', 'trawlers', 'squid_jigger', 'set_longlines', 'set_gillnets',
    'purse_seine', 'pots_and_traps', 'pole_and_line', 'driftnets', 'drifting_longlines']
all_class = fishing_gear + ['bunker', 'cargo', 'specialized_reefer', 'tanker']


var_name = 'fishing_gear'


# each vessel class
if var_name in all_class:
    bar = bar[bar.vessel_class==var_name]
# other variable of interest for fishing vessels
else:
    bar = bar[bar.vessel_class.isin(fishing_gear)]
    # all fishing gear
    if var_name == 'fishing_gear':
        bar = bar
    # flag group
    elif var_name in ['group1', 'group2', 'group3', 'china', 'other']:
        bar = bar[bar.flag_group==var_name]
    # domestic/foreign
    elif var_name == 'domestic':
        bar = bar[bar.flag == bar.port_iso3]
    elif var_name == 'foreign':
        bar = bar[bar.flag != bar.port_iso3]
    # encountered / not enountered
    elif var_name == 'encountered':
        bar = bar[bar.is_encountered]
    elif var_name == 'not_encountered':
        bar = bar[~bar.is_encountered]
    else:
        sys.exit()


# aggregate by port state
baz = pd.DataFrame(bar.groupby(['year', 'port_iso3']).count().start_timestamp)
baz.rename(columns={'start_timestamp': 'n_visits'}, inplace=True)
baz.reset_index(inplace=True)
baz = baz[baz.year.isin([2015, 2017])]

# add PSMA & before/after
baz['psma'] = [1 if any(x == psma_party2016) else 0 for x in baz.port_iso3]
baz['after'] = [0 if x==2015 else 1 for x in baz.year]
keep = np.intersect1d(baz.loc[baz.after==0, 'port_iso3'], baz.loc[baz.after==1, 'port_iso3'])
baz = baz[baz.port_iso3.isin(keep)]


# remove countries/territories with few visits
summary = baz.sort_values('n_visits', ascending=False).copy()
summary['proportion'] = np.cumsum(summary.n_visits)/np.sum(summary.n_visits)
cutoff = summary.loc[summary.proportion > 0.99, 'n_visits'].values[0]
remove_iso3 = summary.loc[summary.n_visits < cutoff, 'port_iso3'].unique()

baz = baz[~baz.port_iso3.isin(remove_iso3)]

#----------------------------
# prepare model input

# design matrix for fixed effects
X = patsy.dmatrix('1 + psma * after', data=baz, return_type='dataframe')
X = np.asarray(X)

# design matrix for random effects
Z = patsy.dmatrix('0 + port_iso3', data=baz, return_type='dataframe')
Z = np.asarray(Z)

# response
Y = np.asarray(baz['n_visits'])
Y_scaled = Y/np.max(Y)


#---------------------------
# model
with pm.Model() as model:

    # fixed effects
    beta_X = pm.Normal('beta_X', mu=0, sigma=100, shape=4)
    mu_X = pm.math.dot(X, beta_X)

    # random intercept
    sigma_Z = pm.HalfCauchy('sigma_Z', beta=5)
    gamma_Z_offset = pm.Normal('gamma_Z_offset', mu=0, sigma=1, shape=Z.shape[1])
    gamma_Z = pm.Deterministic('gamma_Z', gamma_Z_offset * sigma_Z)
    mu_Z = pm.math.dot(Z, gamma_Z)

    ## likelihood
    sigma = pm.HalfCauchy('sigma', beta=5)
    mu_ = mu_X + mu_Z
    #mu = pm.math.exp(mu_)
    #y = pm.Gamma('y', alpha=sigma, beta=sigma/mu, observed=Y)
    y = pm.Lognormal('y', mu=mu_, sigma=sigma, observed=Y_scaled)


#----------------------------
# sample
with model:
    trace = pm.sample(5000, tune=2000, chains=2, target_accept=0.9, return_inferencedata=True)



# save
file_name = 'data/psma/trace/' + var_name + '.nc'
trace.to_netcdf(file_name)


#----------------------------
# summary table
before = baz[baz.year==2015].copy()
after = baz[baz.year==2017].copy()

before.set_index('port_iso3', inplace=True)
after.set_index('port_iso3', inplace=True)


 # summary
summary = pd.DataFrame(index=before.index)
summary['port_state'] = [coco.convert(names=x, to='names') for x in summary.index]
summary['psma_2016'] = before.psma
summary['n_visits_2015'] = before.n_visits
summary['n_visits_2017'] = after.n_visits
obs = np.log(after.n_visits/np.max(Y))
pred = np.log(before.n_visits/np.max(Y)) + np.median(trace.posterior.beta_X, axis=(0,1))[2]
summary['relative_change'] = np.exp(obs - pred)
summary.sort_values('relative_change', inplace=True)

# save
file_name = 'data/psma/rank/' + var_name + '.csv'
summary.to_csv(file_name)
