import logging
import time
from .xmrg_processing import xmrg_processing_geopandas
from .xmrg_utilities import download_files, file_list_from_date_range
from .xmrg_results import xmrg_results
from .xmrgfileiterator.xmrg_file_iterator import xmrg_file_iterator


class xmrg_file_processing:
    def __init__(self, **kwargs):
        self._xmrg_proc = xmrg_processing_geopandas()
        self._xmrg_proc.setup(worker_process_count=kwargs['worker_process_count'],
                    min_latitude_longitude=kwargs['min_latitude_longitude'],
                    max_latitude_longitude=kwargs['max_latitude_longitude'],
                    save_all_precip_values=kwargs["save_all_precip_values"],
                    boundaries=kwargs['boundaries'],
                    source_file_working_directory=kwargs['source_file_working_directory'],
                    delete_source_file=kwargs['delete_source_file'],
                    delete_compressed_source_file=kwargs['delete_compressed_source_file'],
                    kml_output_directory=kwargs['kml_output_directory'],
                    callback_function=self.process_results_callback,
                    base_log_output_directory=kwargs['base_log_directory'])
        #self._file_list = kwargs.get('file_list', [])
        self._file_list_iterator = kwargs.get('file_list_iterator', xmrg_file_iterator())
        self._copy_file = kwargs.get('copy_source_file', False)
        self._download_directory = "./"
        self._xmrg_url = ""
        self._data_saver = kwargs['data_saver']

        self._logger = logging.getLogger(kwargs.get("logger_name", ""))
    @property
    def new_records_added(self):
        return self._data_saver.new_records_added
    @property
    def records_updated(self):
        return self._data_saver.records_updated
    def process_results_callback(self, xmrg_results: xmrg_results):
        self._data_saver.save(xmrg_results)
        return

    def process(self, **kwargs):
        start_time = time.time()
        start_date = kwargs['start_date']
        end_date = kwargs['end_date']
        base_xmrg_directory = kwargs['base_xmrg_directory']

        self._file_list_iterator.setup_iterator(start_date=start_date,
                                                end_date=end_date,
                                                base_xmrg_path=base_xmrg_directory)
        self._logger.info(f"process started. Start date: {start_date} End date: {end_date}")

        self._xmrg_proc.import_files(self._file_list_iterator)

        self._data_saver.finalize()

        self._logger.info(f"process finished in {time.time()-start_time} seconds.")

