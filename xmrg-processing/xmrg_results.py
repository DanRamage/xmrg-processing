class xmrg_results:
    def __init__(self):
        self._datetime = None
        self._boundary_results = {}
        self._boundary_grids = {}

    def add_boundary_result(self, name, result_type, result_value):
        if name not in self._boundary_results:
            self._boundary_results[name] = {}

        results = self._boundary_results[name]
        results[result_type] = result_value

    def get_boundary_results(self, name):
        return (self._boundary_results[name])

    def add_grid(self, boundary_name, grid_tuple):

        if boundary_name not in self._boundary_grids:
            self._boundary_grids[boundary_name] = []

        grid_data = self._boundary_grids[boundary_name]
        grid_data.append(grid_tuple)

    def get_boundary_grid(self, boundary_name):
        grid_data = None
        if boundary_name in self._boundary_grids:
            grid_data = self._boundary_grids[boundary_name]
        return grid_data

    def get_boundary_data(self):
        for boundary_name, boundary_data in self._boundary_results.items():
            yield (boundary_name, boundary_data)

    def get_boundary_names(self):
        return self._boundary_grids.keys()
