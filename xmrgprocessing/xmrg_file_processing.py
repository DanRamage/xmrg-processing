from .xmrg_processing import xmrg_processing_geopandas
from .xmrg_utilities import download_files, file_list_from_date_range



class xmrg_file_processing:
    def __init__(self, **kwargs):
        self._xmrg_proc = xmrg_processing_geopandas()
        self._xmrg_proc.setup(worker_process_count=kwargs['worker_process_count'],
                    min_latitude_longitude=kwargs['min_latitude_longitude'],
                    max_latitude_longitude=kwargs['max_latitude_longitude'],
                    save_all_precip_values=kwargs["save_all_precip_values"],
                    boundaries=kwargs['boundaries'],
                    delete_source_file=kwargs['delete_source_file'],
                    delete_compressed_source_file=kwargs['delete_compressed_source_file'],
                    kml_output_directory=kwargs['kml_output_directory'],
                    callback_function=self.process_results_callback,
                    base_log_output_directory=kwargs['base_log_directory'])
        self._file_list = kwargs.get('file_list', [])
        self._download_directory = "./"
        self._xmrg_url = ""
        self._data_saver = kwargs['data_saver']
    def process_results_callback(self, xmrg_results):
        self._data_saver.save(xmrg_results)
        return

    def process(self, **kwargs):
        start_date = kwargs['start_date']
        end_date = kwargs['end_date']
        download_directory = kwargs['download_directory']
        xmrg_url = kwargs['xmrg_url']

        hours_delta = int((end_date - start_date).seconds / 3600)
        if hours_delta < 1:
            hours_delta = 1
        file_list = file_list_from_date_range(start_date, hours_delta)

        self._file_list = download_files(file_list, download_directory, xmrg_url)

        self._xmrg_proc.import_files(self._file_list)

