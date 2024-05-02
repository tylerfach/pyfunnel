class Funnel:
    def __init__(self, name, stages):
        """
        Creates a funnel class
        
        Parameters
        ----------
        name : str
            name for the funnel
        stages : list
            ordered list of the stages to model
        """
        self.name = name
        self.stages = stages
        
    def format_data(self, data, delta_units = 'day', aggregation_units = 'month'):
        """
        Format the data and add necessary columns for analysis. 
        
        Expecting the columns ['id','stage','stage_value','stage_weight','stage_ts']
                
        Parameters
        ----------
        data : polars.DataFrame
            Polars df containing the historical data for fitting
        delta_units: str
            String representing the timedelta to use for fitting the time between events
            Supported units = 'day' (default), 'hour', 'minute', 'second' 
            
        Returns
        -------
        self.data : polars df
            Polars DataFrame containing the loaded file
        """
        
        assert data.columns == ['id','stage','stage_value','stage_weight','stage_ts'], "Expecting the columns ['id','stage','stage_value','stage_weight','stage_ts']"
        self.data = data
        
        # create an ordered mapping of the stages
        self.stage_map = pl.DataFrame({
            "step_number": list(range(0,len(self.stages),1)),
            "stage": self.stages
        })

        # join the stage mapping onto the df
        self.data = self.data.with_columns(
            step_number = self.data.select('stage').join(self.stage_map, on="stage", how="left").to_series(1)
        )
        
        
        assert delta_units in ['day', 'hour', 'minute', 'second'], "Expecting delta_units to be in ['day', 'hour', 'minute', 'second']"
        match delta_units:
            case 'day':
                time_divisor = 1*60*60*24
            case 'hour':
                time_divisor = 1*60*60
            case 'minute':
                time_divisor = 1*60
            case 'second':
                time_divisor = 1
                
        assert aggregation_units in ['quarter', 'month', 'day'], "Expecting aggregation_units to be in ['quarter', 'month', 'day']"
        match aggregation_units:
            case 'quarter':
                agg_unit = '1q'
            case 'month':
                agg_unit = '1mo'
            case 'day':
                agg_unit = '1d'
        
        # create the time_to_next_stage and the aggregation for reporting
        self.data = self.data.sort('stage_ts', descending= False)
        self.data = self.data.with_columns(
            previous_stage_ts = pl.col('stage_ts').sort_by('stage_ts', descending= False).shift(1).over('id')
        ).with_columns(
            time_to_next_stage = (pl.col('stage_ts') - pl.col('previous_stage_ts')).dt.seconds()/time_divisor
            , stage_ts_agg = pl.col('stage_ts').dt.truncate(agg_unit)
        )