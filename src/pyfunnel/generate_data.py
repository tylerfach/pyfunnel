import polars as pl
import scipy.stats as st
import uuid
import numpy as np
import datetime as dt
import matplotlib.pyplot as plt
plt.style.use('darn_good_style.mplstyle')

#%%

class Demo:
    def __init__(self, name = 'Demo Funnel', stages = ['New Lead','Pipeline Generated','Closed Won']):
        """
        Demo of the package with generated data        

        """
        self.name = name
        self.stages = stages

lead_df = pl.DataFrame({
    "month_start": pl.date_range(dt.date(2023, 7, 1), dt.date(2024, 3, 1), "1mo", eager=True).alias("month_start")
    , "month_end": pl.date_range(dt.date(2023, 7, 31), dt.date(2024, 3, 31), "1mo", eager=True).alias("month_end")
    # , 'new_leads': (86,70,85,118,84,65,82,87,87)
    , 'new_leads': (86,70,85,100,84,65,82,87,87)
})
lead_df

#%% 

def initial_observations(num, start_date, end_date):
    """
    Generate IDs to represent the items to pass through the funnel
    Assumes a uniform distribution (sample with replacement) between the date ranges
    """
    ids = [str(uuid.uuid4()) for x in range(num)]
    dates_to_pick_from = pl.date_range(start_date, end_date, "1d", eager=True)
    
    
    out = pl.DataFrame({
        "id": ids
        ,  "stage_ts":  dates_to_pick_from.sample(n=num, with_replacement = True)   
    }).with_columns(
        pl.lit('New Lead').alias('stage')
        , pl.lit(0.0).alias('stage_value')
        , pl.lit(0.0).alias('stage_weight')
    ).select(
        pl.col('id'), pl.col('stage'), pl.col('stage_value'), pl.col('stage_weight'), pl.col('stage_ts')
    )

    return out


def funnel_pass(df, stage_name, conversion_rate, time_to_convert_distribution, value_distribution = None):
    """
    Pass a column of leads through a stage of the funnel
    
    df: [ids, date]
    
    conversion_rate: percentage that you expect to convert to the next stage
    time_to_convert_distribution: distribution of the time to next stage
    value_distribution: distribution of the value
        If none - don't create values
    """
    
    id_cnt = df.shape[0]
    con_cnt = round(conversion_rate * id_cnt)
    time_to_convert = time_to_convert_distribution.rvs(con_cnt).round()
    
    
    out = df.sample(n=con_cnt, with_replacement = False)
    out = out.with_columns(
        date = pl.col('stage_ts') +  pl.duration(days=time_to_convert)
        , stage = pl.lit(stage_name)
    )
    out
    
    if value_distribution is not None:
        out = out.with_columns(
            stage_value = value_distribution.rvs(size = con_cnt)
        ).select(
            pl.col('id'), pl.col('stage'), pl.col('stage_value').cast(pl.Float64), pl.col('stage_weight'), pl.col('stage_ts')
        )  
        return df.vstack(out)
        
    else:
        return df.vstack(out.select(
            pl.col('id'), pl.col('stage'), pl.col('stage_value'), pl.col('stage_weight'), pl.col('stage_ts')
        ))   

def temp_funnel(num, start_date, end_date, fitted_params):
    """
    Temporary funnel 
    """
    # initial obs
    fit_obs = initial_observations(num, start_date, end_date)

    # pipeline
    fit_obs_pipeline = funnel_pass(fit_obs
                                 , conversion_rate = 0.35
                                   # fitted_params['pipeline_generated_rate']
                                 , time_to_convert_distribution = fitted_params['fit_ttp']
                                 , value_distribution = None)
    # close
    fit_obs_closed_won = funnel_pass(fit_obs_pipeline
                                 , conversion_rate = fitted_params['close_rate']
                                 , time_to_convert_distribution = fitted_params['fit_ttc']
                                 , value_distribution = fitted_params['fit_acv_uniform'])
    fit_obs_closed_won
    return fit_obs_closed_won

# Initial data frame for the leads
lead_df = pl.DataFrame({
    "month_start": pl.date_range(dt.date(2023, 1, 1), dt.date(2023, 9, 1), "1mo", eager=True).alias("month_start")
    , "month_end": pl.date_range(dt.date(2023, 1, 31), dt.date(2023, 9, 30), "1mo", eager=True).alias("month_end")
    , 'new_leads': (120,124,165,131,129,138,140,135, 142)
})



# Time Distributions
stage_1_time_dist = st.gamma(a = 1.5, scale = 7)
stage_2_time_dist = st.gamma(a = 0.8, loc = 0, scale = 42)

# Value distribution
product_values = [1000, 5000, 20000]
probabilities = [0.6, 0.35, 0.05]
acv_dist = st.rv_discrete(values=(product_values, probabilities))
# acv_dist.rvs(size=10)

# Conversion Rates
stage_1_rate = 0.42
stage_2_rate = 0.17


# Generate Data
num = 8
start_date = dt.date(2023,1,1)
end_date = dt.date(2023,1,31)
df = initial_observations(num = num, start_date = start_date, end_date = end_date)
df = funnel_pass(df, stage_name = 'Pipeline Generated', conversion_rate = stage_1_rate, time_to_convert_distribution = stage_1_time_dist, value_distribution = acv_dist)
df = funnel_pass(df, stage_name = 'Closed', conversion_rate = stage_2_rate, time_to_convert_distribution = stage_2_time_dist)
# the DFs aren't stacking correctly because i built it one stage, need to preserve the current stage
# otherwise it's sampling from the full DF instead of the previous stage
print(df)


# 1.99
z = st.gamma(a = 0.8, loc = 0, scale = 42)
z = ttp.rvs(100)
plt.figure(figsize=(6, 6))
plt.hist(z, bins=25, density=True, alpha=0.6, color='g')
plt.show()
# Plot the PDF.
# df = initial_observations(num,start_date,end_date)
# funnel_pass(df, conversion_rate = 0.45, time_to_convert_distribution = ttp, value_distribution = acv_dist)



# for row in temp_df.rows(named=True):
    # temp = temp_funnel(num = row['new_leads'], start_date = row['month_start'], end_date = row['month_end'], fitted_params = temp_params)
