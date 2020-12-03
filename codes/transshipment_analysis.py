import time
import numpy as np
import pandas as pd
import xgboost as xgb
import shap
import itertools
import scipy


#_________________________________
# run transshipment_trips.sql and save as transhipment_trips.csv
encounter = pd.read_csv('transshipment_trips.csv')

# run transshipment_loitering.sql and save as transhipment_loitering.csv
loitering = pd.read_csv('transshipment_loitering.csv')


#_________________________________
# trips with predictors
all = encounter.dropna(subset=['carrier_flag_group','neighbor_flag_group', 'time_at_sea', 'neighbor_vessel_class']).copy()
all.reset_index(inplace=True, drop=True)


# data frame for predictors
foo = all.groupby('gfw_trip_id').first()


# add time at sea
tas = all.time_at_sea.unique()
for i in range(len(tas)):
    foo[tas[i]] = [1 if x == tas[i] else 0 for x in foo.time_at_sea]


# add flags of carrier vessels
carrier_flags = all.carrier_flag_group.unique()
for i in range(len(carrier_flags)):
    foo[carrier_flags[i]] = [1 if x == carrier_flags[i] else 0 for x in foo.carrier_flag_group]


foo = foo[np.append(tas, carrier_flags)]


# add flags of encountered fishing vessels
bar = pd.DataFrame()
bar['gfw_trip_id'] = all.gfw_trip_id
neighbor_flags = all.neighbor_flag_group.unique()
for i in range(len(neighbor_flags)):
    bar['with_' + neighbor_flags[i]] = [1 if x == neighbor_flags[i] else 0 for x in all.neighbor_flag_group]


# add vessel class of encountered fishing vessels
neighbor_vessels = all.neighbor_vessel_class.unique()
for i in range(len(neighbor_vessels)):
    bar['with_' + neighbor_vessels[i]] = [1 if x == neighbor_vessels[i] else 0 for x in all.neighbor_vessel_class]


# summarize within trip id (0: no encounter, 1: encountred)
bar = bar.groupby('gfw_trip_id').sum()
bar = bar.applymap(lambda x: (x > 0)*1) #convert n > 0 to n = 1
foo = foo.merge(bar, left_index=True, right_index=True)


# number of loitering
foo['loitering'] = loitering.groupby('gfw_trip_id').count().ssvid
foo['loitering'] = foo.loitering.fillna(0)
foo['loitering'] = [1 if x > 0 else 0 for x in foo.loitering]
foo['no_loitering'] = 1 - foo.loitering


# get a subset with port risk assessment
bar = all.groupby('gfw_trip_id').first()
bar['n_total_to'] = bar.to_iuu_no + bar.to_iuu_low + bar.to_iuu_med + bar.to_iuu_high
## for labor abuse
## bar['n_total_to'] = bar.to_la_no + bar.to_la_low + bar.to_la_med + bar.to_la_high
obs = foo[bar.n_total_to > 0].copy()


# risk score
obs['risk_score'] = 1/3*bar.to_iuu_low + 2/3*bar.to_iuu_med + bar.to_iuu_high - bar.to_iuu_no
## for labor abuse
## obs['risk_score'] = 1/3*bar.to_la_low + 2/3*bar.to_la_med + bar.to_la_high - bar.to_la_no
obs['type'] = 'obs'


# input for the model to predict missing port risk score
x_obs = obs.drop(columns=['risk_score', 'type']).copy()
y_obs = obs.risk_score.astype('float')
dtrain = xgb.DMatrix(data=x_obs,label=y_obs)


# fit model
params = {'eta':0.01, 'min_child_weight':1, 'max_depth':10, 'colsample_bytree':0.6}
n_trees = 300


evals_result = {}
bst = xgb.train(params=params, dtrain=dtrain, num_boost_round=n_trees, evals=[(dtrain, 'train')],
    verbose_eval=50, evals_result=evals_result)


#______________________________________
# predict
bar = foo.drop(obs.index).copy()

x = xgb.DMatrix(bar)
y_pred = bst.predict(x)
y_pred = pd.DataFrame(y_pred, columns=['risk_score'], index=bar.index)
bar['risk_score'] = y_pred
bar['type'] = 'pred'

# combine observed and predicted risk scores
foo = pd.concat([obs, bar])

# add coordinates
encounter.set_index('gfw_trip_id', inplace=True)
encounter['risk_score'] = foo.risk_score
encounter.dropna(subset=['risk_score'], inplace=True)
x = encounter[['lon_mean', 'lat_mean', 'risk_score']].copy()

loitering.set_index('gfw_trip_id', inplace=True)
loitering['risk_score'] = foo.risk_score
loitering.dropna(subset=['risk_score'], inplace=True)
y = loitering[['lon_mean', 'lat_mean', 'risk_score']].copy()

xy = pd.concat([x, y])
threshold = [0,2]
xy['risk_class'] = [0 if x < threshold[0] else 1 if x < threshold[1] else 2 for x in xy.risk_score]


#________________________________________________
# SHAP interaction values

explainer = shap.TreeExplainer(bst)
shap_value = explainer.shap_interaction_values(x_obs)


#________________________________
# feature importance

is_tas_idx = slice(0,5,1)
is_flag_idx = slice(5,10,1)
with_flag_idx = slice(10,15,1)
with_gear_idx = slice(15,23,1)
loitering_idx = slice(23,25,1)

col_idx = list(itertools.combinations_with_replacement([is_tas_idx, is_flag_idx, with_flag_idx, with_gear_idx, loitering_idx],2))
col_name = list(itertools.combinations_with_replacement(['is_tas', 'is_flag', 'with_flag', 'with_gear', 'loitering'],2))


foo = [[shap_value[i,x1,x2].sum() for (x1,x2) in col_idx] for i in range(x_obs.shape[0])]
foo = pd.DataFrame(foo)
foo.columns = col_name


# summarize
importance = pd.DataFrame()
importance['mean'] = foo.abs().mean(axis=0)
importance['sd'] = foo.abs().std(axis=0)
importance['se'] = foo.abs().sem(axis=0)
importance['lower'] = foo.abs().quantile(q=0.025, axis=0)
importance['upper'] = foo.abs().quantile(q=0.975, axis=0)


#___________________________
# effect of features when present

# model baseline
X = xgb.DMatrix(x_obs)
y_pred = bst.predict(X)
base = np.mean(y_pred)

col_idx = list(itertools.combinations_with_replacement([is_tas_idx, is_flag_idx,
    10,11,12,13,14,15,16,17,18,19,20,21,22,loitering_idx],2))


col_name = list(itertools.combinations_with_replacement(['is_tas', 'is_flag', 'with_china', 'with_group3',
    'with_group2', 'with_other', 'with_group1', 'with_squid_jigger',
    'with_set_longline', 'with_drifting_longline', 'with_pots_and_traps',
    'with_trawlers', 'with_purse_seine', 'with_pole_and_line',
    'with_set_gillnet', 'loitering'],2))


foo = [[shap_value[i,x1,x2].sum() for (x1,x2) in col_idx] for i in range(x_obs.shape[0])]
foo = pd.DataFrame(foo)
foo.columns = col_name


## combination of features
a = list(itertools.combinations_with_replacement(x_obs.columns,2))
b1 = list(itertools.combinations(x_obs.columns[is_tas_idx],2))
b2 = list(itertools.combinations(x_obs.columns[is_flag_idx],2))
b3 = list(itertools.combinations(x_obs.columns[loitering_idx],2))
combo = list(set(a).difference(set(b1 + b2 + b3)))


# corresponding class
x = ['is_tas']*5 + ['is_flag']*5 + ['with_china', 'with_group3',
    'with_group2', 'with_other', 'with_group1', 'with_squid_jigger',
    'with_set_longline', 'with_drifting_longline', 'with_pots_and_traps',
    'with_trawlers', 'with_purse_seine', 'with_pole_and_line',
    'with_set_gillnet'] + ['loitering']*2
combo2 = [(x[x_obs.columns.get_loc(x1)],x[x_obs.columns.get_loc(x2)]) for (x1,x2) in combo]


# select solo features
solo_idx = list(np.where([x1==x2 for (x1,x2) in combo])[0])


# summarize
mean = []; sd = []; se = []; lower = []; upper = []
for x in solo_idx:

    bar = foo[combo2[x]]  # SHAP values of solo features
    true_idx = list(np.where(x_obs[combo[x][0]]==1)[0])   # find where it is true
    bar2 = bar[true_idx]

    if len(bar2) > 1:
        mean.append(np.mean(bar2) + base)
        sd.append(np.std(bar2))
        se.append(scipy.stats.sem(bar2))
        lower.append(np.quantile(bar2, 0.025) + base)
        upper.append(np.quantile(bar2, 0.975) + base)

    if len(bar2) <= 1:
        mean.append(np.nan)
        sd.append(np.nan)
        se.append(np.nan)
        lower.append(np.nan)
        upper.append(np.nan)

solo_effect = pd.DataFrame()
solo_effect['mean'] = mean
solo_effect['sd'] = sd
solo_effect['se'] = se
solo_effect['lower'] = lower
solo_effect['upper'] = upper
solo_effect.index = [combo[x][0] for x in solo_idx]
solo_effect.dropna(inplace=True)


# select combo features
combo_idx = list(np.where([x1!=x2 for (x1,x2) in combo])[0])


# summarize
mean = []; sd = []; se = []; lower = []; upper = []
for x in combo_idx:

    bar1 = np.array(foo[(combo2[x][0], combo2[x][0])]) + np.array(foo[combo2[x]])
    bar2 = np.array(foo[(combo2[x][1], combo2[x][1])]) + np.array(foo[combo2[x]])
    bar = bar1 + bar2
    # find where it is true
    true_idx = list(np.where(np.logical_and(x_obs[combo[x][0]]==1, x_obs[combo[x][1]]==1))[0])
    bar2 = bar[true_idx]

    if len(bar2) > 1:
        mean.append(np.mean(bar2) + base)
        sd.append(np.std(bar2))
        se.append(scipy.stats.sem(bar2))
        lower.append(np.quantile(bar2, 0.025) + base)
        upper.append(np.quantile(bar2, 0.975) + base)

    if len(bar2) <= 1:
        mean.append(np.nan)
        sd.append(np.nan)
        se.append(np.nan)
        lower.append(np.nan)
        upper.append(np.nan)

combo_effect = pd.DataFrame()
combo_effect['mean'] = mean
combo_effect['sd'] = sd
combo_effect['se'] = se
combo_effect['lower'] = lower
combo_effect['upper'] = upper
combo_effect.index = [combo[x] for x in combo_idx]
combo_effect.dropna(inplace=True)

effect = pd.concat([solo_effect, combo_effect])
