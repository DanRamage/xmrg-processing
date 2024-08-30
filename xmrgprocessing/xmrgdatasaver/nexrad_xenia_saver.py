import logging

from .nexrad_data_saver import precipitation_saver
from xeniadbutilities.xeniaSQLiteAlchemy import xeniaAlchemy, multi_obs, platform
from datetime import datetime
import sqlite3
from sqlalchemy import select, update, exc
import time
from shapely.ops import unary_union


class nexrad_xenia_sqlite_saver(precipitation_saver):
    def __init__(self, sqlite_file):
        self._xenia_db = xeniaAlchemy()
        self._xenia_db.connect_sqlite_db(sqlite_file, False)
        self._check_exists = True
        self._save_all_precip_values = True
        self._add_sensors = True
        self.sensor_ids = {}
        self.row_entry_date = datetime.now()
        self._logger = logging.getLogger()

    def check_exists(self, platform_handle, xmrg_results_data):
        org, platform_name, platform_type = platform_handle.split('.')
        self._logger.info(f"Checking organisation: {org} and platforms: {platform_handle} exist.")
        org_id = self._xenia_db.organizationExists(org)
        if org_id is None:
            org_id = self._xenia_db.addOrganization(rowEntryDate=self.row_entry_date, organizationName=org)
        # Add the platforms to represent the watersheds and drainage basins
        if self._xenia_db.platformExists(platform_handle) is None:
            self._logger.info(f"Adding platform. Org: {org_id} Platform Handle: {platform_handle} "
                               f"Short_Name: {platform_name}")
            # Figure out the center of the boundaries, we'll then use that for the latitude and longitude
            # of the platform.
            boundary_grid_data = xmrg_results_data.get_boundary_grid(platform_name)
            poly_list = [x[0] for x in boundary_grid_data]
            combined_polygons = unary_union(poly_list)
            centroid = combined_polygons.centroid
            platform_rec = platform(
                row_entry_date=self.row_entry_date,
                platform_handle=platform_handle,
                short_name=platform_name,
                fixed_latitude=centroid.y,
                fixed_longitude=centroid.x,
                organization_id=org_id
            )
            try:
                self._xenia_db.session.add(platform_rec)
                self._xenia_db.session.commit()
            except Exception as e:
                self._xenia_db.session.rollback()
                self._logger.error(f"Failed to add platform: {platform_handle} for org_id: {org_id}, cannot continue")
                self._logger.exception(e)
        if self._add_sensors:
            self._xenia_db.addNewSensor('precipitation_radar_weighted_average', 'mm',
                                        platform_handle,
                                        1,
                                        0,
                                        1, None, True)

        return

    def save(self, xmrg_results_data):
        try:
            for boundary_name, boundary_results in xmrg_results_data.get_boundary_data():
                '''
                if self.writePrecipToKML and xmrg_results_data.get_boundary_grid(boundary_name) is not None:
                    self.write_boundary_grid_kml(boundary_name, xmrg_results_data)
    
                if self.kmlTimeSeries:
                    self.kml_time_series.append(xmrg_results_data)
                '''
                platform_handle = "nws.%s.radarcoverage" % (boundary_name)
                self._logger.info(f"Saving platform: {platform_handle} {xmrg_results_data.datetime}")
                if self._check_exists:
                    self.check_exists(platform_handle, xmrg_results_data)
                lat = 0.0
                lon = 0.0

                avg = boundary_results['weighted_average']
                if avg != None:
                    if avg > 0.0 or self._save_all_precip_values:
                        if avg != -9999:
                            # Build a dict of m_type and sensor_id for each platform to make the inserts
                            # quicker.
                            if platform_handle not in self.sensor_ids:
                                try:
                                    platform_info = self._xenia_db.session.query(platform) \
                                        .filter(platform.platform_handle == platform_handle) \
                                        .one()
                                except Exception as e:
                                    self._logger.exception(e)
                                else:
                                    m_type_id = self._xenia_db.mTypeExists('precipitation_radar_weighted_average',
                                                                           'mm')
                                    sensor_id = self._xenia_db.sensorExists('precipitation_radar_weighted_average',
                                                                            'mm',
                                                                            platform_handle, 1)
                                    self.sensor_ids[platform_handle] = {
                                        'latitude': platform_info.fixed_latitude,
                                        'longitude': platform_info.fixed_longitude,
                                        'm_type_id': m_type_id,
                                        'sensor_id': sensor_id}

                            # Add the avg into the multi obs table. Since we are going to deal with the hourly data for the radar and use
                            # weighted averages, instead of keeping lots of radar data in the radar table, we calc the avg and
                            # store it as an obs in the multi-obs table.
                            add_obs_start_time = time.time()
                            db_rec = multi_obs(
                                row_entry_date=self.row_entry_date,
                                platform_handle=platform_handle,
                                sensor_id=self.sensor_ids[platform_handle]['sensor_id'],
                                m_type_id=self.sensor_ids[platform_handle]['m_type_id'],
                                m_date=xmrg_results_data.datetime,
                                m_lon=self.sensor_ids[platform_handle]['latitude'],
                                m_lat=self.sensor_ids[platform_handle]['longitude'],
                                m_value=avg
                            )
                            try:
                                self._xenia_db.session.add(db_rec)
                                self._xenia_db.session.commit()
                            # Trying to add record that already exists.
                            except exc.IntegrityError as e:
                                self._xenia_db.session.rollback()
                                self._logger.error("Record already exists, updating.")
                                try:
                                    self._xenia_db.session.query(multi_obs)\
                                        .filter(multi_obs.platform_handle == platform_handle) \
                                        .filter(multi_obs.m_date == xmrg_results_data.datetime) \
                                        .filter(multi_obs.m_type_id == self.sensor_ids[platform_handle]['m_type_id']) \
                                        .filter(multi_obs.sensor_id == self.sensor_ids[platform_handle]['sensor_id']) \
                                        .update({"m_value": avg})
                                    self._xenia_db.session.commit()
                                except Exception as e:
                                    self._logger.exception(e)
                                else:
                                    self._logger.debug(
                                        f"Platform: {platform_handle} Date: {xmrg_results_data.datetime} updated "
                                        f"weighted avg: {avg} in {time.time() - add_obs_start_time} seconds.")

                        else:
                            self._logger.debug(
                                f"Platform: {platform_handle} Date: {xmrg_results_data.datetime} weighted avg: {avg}(mm)"
                                f" is not valid, not adding to database.")
                    else:
                        self._logger.debug(
                            f"Platform: {platform_handle} Date: {xmrg_results_data.datetime} configuration parameter "
                            f"not set to add precip values of 0.0.")
                else:
                    self._logger.error(f"Platform: {platform_handle} Date: {xmrg_results_data.datetime} "
                                       f"Weighted AVG error")

            self._check_exists = False
        except Exception as e:
            self._logger.exception(e)
        return
