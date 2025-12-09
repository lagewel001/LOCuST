class CodeLabelMapper(dict):
    """A 'two-way' dictionary for mapping codes to their friendly names and the other way around."""
    def __init__(self, d):
        super().__init__(d)
        self.inv: dict  = {v: k for k, v in d.items()}  # add inverse of d

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        self.inv.__setitem__(value, key)

    def __delitem__(self, key):
        self.inv.__delitem__(self[key])
        super().__delitem__(key)
