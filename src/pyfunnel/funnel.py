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
        