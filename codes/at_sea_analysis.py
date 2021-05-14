import time
import numpy as np
import pandas as pd
import xgboost as xgb
import shap
import itertools
import scipy
import datatable as dt


# output of fishing_trips.sql 
data = dt.fread('fishing_trips.csv')


# trips with flag, gear, time at sea
all = data.copy()
del data
all = all[dt.f.flag_group != '', :]
all = all[dt.f.vessel_class != '', :]
all = all[dt.f.time_at_sea != '', :]


# subset of data with port risk assessment
all = all.to_pandas()
obs = all[all[['iuu_no_to', 'iuu_low_to', 'iuu_med_to', 'iuu_high_to']].sum(axis=1) > 0].copy()

## for labor abuse
## obs = all[all[['la_no_to', 'la_low_to', 'la_med_to', 'la_high_to']].sum(axis=1) > 0].copy()

# add risk score
obs['risk_score'] = 1/3 * obs.iuu_low_to + 2/3 * obs.iuu_med_to + obs.iuu_high_to - obs.iuu_no_to

## for labor abuse
## obs['risk_score'] = 1/3 * obs.la_low_to + 2/3 * obs.la_med_to + obs.la_high_to - obs.la_no_to

obs['type'] = 'obs'


# input for the model to predict missing port risk score
x_obs = pd.get_dummies(obs[['flag_group', 'vessel_class', 'time_at_sea']])
x_obs.reset_index(inplace=True, drop=True)
y_obs = obs.risk_score.astype('float')
y_obs.reset_index(inplace=True, drop=True)
dtrain = xgb.DMatrix(data=x_obs,label=y_obs)


# fit model
params = {'eta':0.05, 'min_child_weight':1, 'max_depth':10, 'colsample_bytree':0.6}
n_trees = 100

evals_result = {}
bst = xgb.train(params=params, dtrain=dtrain, num_boost_round=n_trees, evals=[(dtrain, 'train')],
    verbose_eval=10, evals_result=evals_result)


#-----------------------------
# prediction error
#-----------------------------
x = xgb.DMatrix(x_obs)
y_pred = bst.predict(x)

# prediction
foo = obs[['gfw_trip_id', 'ssvid', 'trip_start', 'trip_end']].copy()
foo['risk_score'] = y_pred
threshold = [0,2]
foo['risk_class'] = [0 if x < threshold[0] else 1 if x < threshold[1] else 2 for x in foo.risk_score]

foo.to_csv('fishing_iuu_pred.csv', index=False)

# for labor abuse,
# foo.to_csv('fishing_la_pred.csv', index=False)

# observation
foo = obs[['gfw_trip_id', 'ssvid', 'trip_start', 'trip_end', 'risk_score']].copy()
foo['risk_class'] = [0 if x < threshold[0] else 1 if x < threshold[1] else 2 for x in foo.risk_score]

foo.to_csv('fishing_iuu_obs.csv', index=False)

# for labor abuse,
# foo.to_csv('fishing_la_obs.csv', index=False)

#-------------------
# predict
#-------------------
# data
pred = all.drop(obs.index)
pred = pred[pred.vessel_class.isin(obs.vessel_class.unique())]
x = pd.get_dummies(pred[['flag_group', 'vessel_class', 'time_at_sea']])
x = xgb.DMatrix(x)

# predict
pred['risk_score'] = bst.predict(x)
pred['type'] = 'pred'


# combine observed and predicted risk scores
bar = pd.concat([pred, obs])
bar = bar[['gfw_trip_id', 'ssvid', 'trip_start', 'trip_end', 'risk_score', 'type']]


# save output for gridding and plotting
bar['risk_class'] = [0 if x < threshold[0] else 1 if x < threshold[1] else 2 for x in bar.risk_score]

bar.to_csv('fishing_iuu.csv', index=False)
           
# for labor abuse,
# bar.to_csv('fishing_la.csv', index=False)


#-----------------------------
# SHAP interaction values
#-----------------------------
           
explainer = shap.TreeExplainer(bst)
shap_value = explainer.shap_interaction_values(x_obs)


# feature importance

flag_idx = slice(0,5,1)
gear_idx = slice(5,14,1)
tas_idx = slice(14,19,1)


col_idx = list(itertools.combinations_with_replacement([flag_idx, gear_idx, tas_idx],2))
col_name = list(itertools.combinations_with_replacement(['flag', 'gear', 'tas'],2))


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

importance.to_csv('fishing_iuu_importance.csv')


# effect of features when present

# model baseline
X = xgb.DMatrix(x_obs)
y_pred = bst.predict(X)
base = np.mean(y_pred)


## sum SHAP values over mutually exclusive features
## because one is present means the others are absent

## combination of features
a = list(itertools.combinations_with_replacement(x_obs.columns,2))
b1 = list(itertools.combinations(x_obs.columns[flag_idx],2))
b2 = list(itertools.combinations(x_obs.columns[gear_idx],2))
b3 = list(itertools.combinations(x_obs.columns[tas_idx],2))
combo = list(set(a).difference(set(b1 + b2 + b3)))


# corresponding class
x = ['flag']*5 + ['gear']*9 + ['tas']*5
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

effect.to_csv('fishing_iuu_effect.csv')
