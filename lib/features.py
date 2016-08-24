Selector = lambda x: {
    'simple': Simple,
    'average': Average,
    'change': PercentageChange,
    'difference': Difference,
}[x]

class FeatureEngineer:
    def select(self, df):
        assert(not df.isnull().values.any())
        return self._select(df).ravel().tolist()

    def select_(self, df):
        raise NotImplementedError

class Simple(FeatureEngineer):
    def _select(self, df):
        return df.values

class Average(FeatureEngineer):
    def _select(self, df):
        return df.mean()

class NAFeatureEngineer(FeatureEngineer):
    def __init__(self):
        self.method = None
        
    def _select(self, df):
        df_ = getattr(df, self.method)()
        
        return df_[1:]
    
class PercentageChange(NAFeatureEngineer):
    def __init__(self):
        super().__init__()
        self.method = 'pct_change'

class Difference(NAFeatureEngineer):
    def __init__(self):
        super().__init__()
        self.method = 'diff'
