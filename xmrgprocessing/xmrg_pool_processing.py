import os
import logging
from multiprocessing import Process, Pool, Queue, current_process
import time
from venv import logger

import pandas as pd
import geopandas as gpd
import shutil

from .xmrg_results import xmrg_results
from .geoXmrg import geoXmrg, LatLong
from .xmrg_utilities import get_collection_date_from_filename

def process_xmrg_file_geopandas_pool(kwargs, **args):
    '''

    :param kwargs:
    :return:
    '''
    try:
        try:
            processing_start_time = time.time()

            gp_results = None

            xmrg_file_count = 1
            logger = None
            process_name = current_process().name

            xmrg_filename = kwargs.get('xmrg_filename')
            #Each worker will get its own log file.
            base_log_output_directory = kwargs.get('base_log_output_directory',
                                                   'process_xmrg_file_geopandas.log')
            log_output_filename = os.path.join(base_log_output_directory,
                                               f"process_xmrg_file_geopandas-{process_name}.log")
            error_log_output_filename = os.path.join(base_log_output_directory,
                                               f"process_xmrg_file_geopandas_errors-{process_name}.log")
            debug_dir = kwargs['debug_files_directory']
            resultsQueue = kwargs['results_queue']
            save_all_precip_vals = kwargs['save_all_precip_vals']
            delete_source_file = kwargs['delete_source_file']
            delete_compressed_source_file = kwargs['delete_compressed_source_file']
            # A course bounding box that restricts us to our area of interest.
            minLatLong = None
            maxLatLong = None
            if 'min_lat_lon' in kwargs and 'max_lat_lon' in kwargs:
                minLatLong = LatLong(kwargs['min_lat_lon'][0], kwargs['min_lat_lon'][1])
                maxLatLong = LatLong(kwargs['max_lat_lon'][0], kwargs['max_lat_lon'][1])

            # Boundaries we are creating the weighted averages for.
            boundaries = kwargs['boundaries']

            logger = logging.getLogger("process_xmrg_file_geopandas")
            # Since the Pools get reused, we only want to set a logger up once.
            if not logger.hasHandlers():
                formatter = logging.Formatter("%(asctime)s,%(levelname)s,%(funcName)s,%(lineno)d,%(message)s")
                fh = logging.handlers.RotatingFileHandler(log_output_filename)
                error_fh = logging.handlers.RotatingFileHandler(error_log_output_filename)
                ch = logging.StreamHandler()
                fh.setLevel(logging.DEBUG)
                error_fh.setLevel(logging.ERROR)
                ch.setLevel(logging.DEBUG)
                fh.setFormatter(formatter)
                ch.setFormatter(formatter)
                logger.addHandler(fh)
                logger.addHandler(ch)


            logger.info(f"{process_name} starting process_xmrg_file_geopandas.")


            save_boundary_grid_cells = True
            save_boundary_grids_one_pass = True
            write_percentages_grids_one_pass = True

        except Exception as e:
            logger.exception(e)

        else:
            # Build boundary dataframes
            boundary_frames = []
            for boundary in boundaries:
                df = pd.DataFrame([[boundary[0], boundary[1]]], columns=['Name', 'Boundaries'])
                boundary_df = gpd.GeoDataFrame(df, geometry=df.Boundaries)
                boundary_df = boundary_df.drop(columns=['Boundaries'])
                boundary_df.set_crs(epsg=4326, inplace=True)
                boundary_frames.append(boundary_df)
                # Write out a geojson file we can use to visualize the boundaries if needed.
                try:
                    boundaries_outfile = os.path.join(debug_dir,
                                                      f"{boundary_df['Name'][0].replace(' ', '_')}_boundary.json")
                    if not os.path.exists(boundaries_outfile):
                        boundary_df.to_file(boundaries_outfile, driver="GeoJSON")
                except Exception as e:
                    logger.exception(e)

            tot_file_time_start = time.time()
            logger.debug(f"{process_name} processing file: {xmrg_filename}")

            gpXmrg = geoXmrg(minLatLong, maxLatLong, 0.01)
            try:
                gpXmrg.openFile(xmrg_filename)
            except Exception as e:
                logger.exception(f"{process_name} Failed to open file: {xmrg_filename}. {e}")
            else:

                # This is the database insert datetime.
                # Parse the filename to get the data time.
                (directory, filetime) = os.path.split(gpXmrg.fileName)
                xmrg_filename = filetime
                (filetime, ext) = os.path.splitext(filetime)
                filetime = get_collection_date_from_filename(filetime)

                try:
                    if gpXmrg.readFileHeader():
                        read_rows_start = time.time()
                        gpXmrg.readAllRows()
                        if logger:
                            logger.info(f"{process_name}({time.time() - read_rows_start} secs)"
                                        f" to read all rows in file: {xmrg_filename}")

                        gp_results = xmrg_results()
                        gp_results.datetime = filetime
                        # overlayed = gpd.overlay(gpXmrg._geo_data_frame, boundary_df, how="intersection")

                        for index, boundary_row in enumerate(boundary_frames):
                            file_start_time = time.time()
                            overlayed = gpd.overlay(boundary_row, gpXmrg._geo_data_frame, how="intersection",
                                                    keep_geom_type=False)

                            if save_boundary_grid_cells:
                                for ndx, row in overlayed.iterrows():
                                    gp_results.add_grid(row.Name, (row.geometry, row.Precipitation))
                            # Here we create our percentage column by applying the function in the map(). This applies to
                            # each area.
                            overlayed['percent'] = overlayed.area.map(
                                lambda area: float(area) / float(boundary_row.area))
                            overlayed['weighted average'] = (overlayed['Precipitation']) * (overlayed['percent'])

                            wghtd_avg_val = sum(overlayed['weighted average'])
                            gp_results.add_boundary_result(boundary_row['Name'][0], 'weighted_average',
                                                           wghtd_avg_val)
                            logger.info(f"{process_name} File: {xmrg_filename} "
                                        f"Processed boundary: {boundary_row.Name[0]} WgtdAvg: {wghtd_avg_val}"
                                        f" in {time.time() - file_start_time} seconds.")
                            xmrg_file_count += 1

                            if write_percentages_grids_one_pass:
                                try:
                                    percentage_file = os.path.join(debug_dir,
                                        f"{overlayed['Name'][0].replace(' ', '_')}_percentage.json")
                                    if not os.path.exists(percentage_file):
                                        overlayed.to_file(percentage_file, driver="GeoJSON")
                                    #Once we've written out each boundary, we can stop.
                                    if index == len(boundary_frames) - 1:
                                        write_percentages_grids_one_pass = False
                                except Exception as e:
                                    logger.exception(e)
                            if save_boundary_grids_one_pass:
                                try:
                                    full_data_grid = os.path.join(debug_dir,
                                                                  "%s_%s_fullgrid_.json" % (
                                                                  filetime.replace(':', '_'),
                                                                  boundary_row.Name[0].replace(' ', '_')))
                                    gpXmrg._geo_data_frame.to_file(full_data_grid, driver="GeoJSON")
                                    save_boundary_grids_one_pass = False
                                except Exception as e:
                                    logger.exception(e)

                        #resultsQueue.put(gp_results)
                        try:
                            gpXmrg.cleanUp(delete_source_file, delete_compressed_source_file)
                        except Exception as e:
                            logger.exception(e)
                    else:
                        logger.error(f"{process_name} Failed to process file: {xmrg_filename}")
                except Exception as e:
                    logger.exception(f"{process_name} Failed to process file: {xmrg_filename}. {e}")
            if logger:
                logger.debug(f"{process_name} process finished. Processed in: "
                             f"{time.time() - processing_start_time} seconds")
    except Exception as e:
        logger.exception(e)

    return gp_results


class xmrg_processing_geopandas:
    def __init__(self):
        self._logger = logging.getLogger()
        self._min_latitude_longitude = None
        self._max_latitude_longitude = None
        self._save_all_precip_values = False
        self._boundaries = []
        self._source_file_working_directory = None
        self._delete_source_file = False
        self._delete_compressed_source_file = False
        self._kml_output_directory = None
        self._callback_function = None
        self._logging_config = None
        self._base_log_output_directory = ""
        self._worker_process_count = 4

    def setup(self, **kwargs):
        #Number of Processes to spawn.
        self._worker_process_count = kwargs.get("worker_process_count", 4)

        #The overall bounding box to trim the XMRG data to.
        self._min_latitude_longitude = kwargs.get("min_latitude_longitude", None)
        self._max_latitude_longitude = kwargs.get("max_latitude_longitude", None)

        #Save all the preciptation values, not just > 0 ones.
        self._save_all_precip_values = kwargs.get("save_all_precip_values", False)

        #The list of boundaries to process rain data for.
        self._boundaries = kwargs.get("boundaries", None)

        #These next parameters deal with where we process the data files. We might be grabbing files
        #from an archive, so we want to copy them to a working directory.
        #If set, copy the XMRG files to this directory for processing.
        self._source_file_working_directory = kwargs.get("source_file_working_directory", None)
        #Delete the source file when it has been processed.
        self._delete_source_file = kwargs.get("delete_source_file", False)
        #Delete the compressed file after processing
        self._delete_compressed_source_file = kwargs.get("delete_compressed_source_file", False)

        #The directory to output the KML file we use for debugging.
        self._kml_output_directory = kwargs.get("kml_output_directory", None)

        #Callback function used when we have a result.
        self._callback_function = kwargs.get("callback_function", None)

        #Directory where logfiles are written.
        self._base_log_output_directory = kwargs.get("base_log_output_directory", "")

    def import_files(self, file_list_iterator):
        self._logger.debug("Start import_files")

        input_queue = []

        rec_count = 0
        for xmrg_file in file_list_iterator:
            file_to_process = xmrg_file
            # Copy the file to our local working directory
            if self._source_file_working_directory is not None:
                try:
                    xmrg_src_dir, xmrg_src_filename = os.path.split(xmrg_file)
                    source_fullfilepath = os.path.join(self._source_file_working_directory, xmrg_src_filename)
                    shutil.copy2(xmrg_file, self._source_file_working_directory)
                    file_to_process = source_fullfilepath
                except Exception as e:
                    logger.exception(e)
                    file_to_process = None
            if file_to_process is not None:
                input_queue.append({
                    'xmrg_filename': file_to_process,
                    'min_lat_lon': self._min_latitude_longitude,
                    'max_lat_lon': self._max_latitude_longitude,
                    'save_all_precip_vals': self._save_all_precip_values,
                    'boundaries': self._boundaries,
                    'delete_source_file': self._delete_source_file,
                    'delete_compressed_source_file': self._delete_compressed_source_file,
                    'debug_files_directory': self._kml_output_directory,
                    'base_log_output_directory': self._base_log_output_directory
                })
                try:
                    with Pool(self._number_of_workers) as pool:
                        for results in pool.imap_unordered(process_xmrg_file_geopandas_pool, self._input_queue):
                            for result in results:
                                rec_count += 1
                                self.process_result(result)

                except Exception as e:
                    self._logger.exception(e)




        input_queue.put('STOP')


        self._logger.info(f"Imported: {rec_count} records")

        self._logger.debug("Finished import_files")

        return

    def process_result(self, xmrg_results_data):
        if self._callback_function is not None:
            self._callback_function(xmrg_results_data)
        return