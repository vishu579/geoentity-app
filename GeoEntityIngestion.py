# -*- coding: utf-8 -*-
#------------------------------------#
# Project Description                #
#------------------------------------#
__dateCreated__= "Feb 16 2023"
__project__= "Krishi DSS for Ministry of Agriculture"
__module__= "Geo Entity Ingestionl Module"
__author__= "Nitin Mishra"
__internal_code_reviewer__= "Pankaj Bodani"
__entity__= "CGDD/VRG/EPSA"
__organization__= "SAC/ISRO"
__purpose__= "This submodule is developed for ingesting GeoEntity in Zonal Stats Database."

#------------------------------------#
# Module Import                      #
#------------------------------------#
import json # To read config json
import geopandas as gpd # For GeoJSON
import psycopg2 # For PostgreSQL Database Queries
import sys # For Runtime Environment
import os # OS kernel module access
import datetime
import calendar
import time
import math
import numpy as np


class GeoEntityIngest:
    #------------------------------------#
    # Private Members                    #
    #------------------------------------#    
    __config_file_path=None

    #------------------------------------#
    # Aux Methods                        #
    #------------------------------------#
    
    def __printMsg(self,opt,text):
        """
        Purpose
        ----------
        This method will Print output based on selected option

        Parameters
        ----------
        opt : Options like: 'Warning','Info','Error'
        text: Text which will be printed

        Returns
        -------
        Printed text as per the selected option.
        """
        if opt=="Warning":
            print("[Warning]: "+text+"\r\n")
        elif opt=="Info":
            print("[Info]: "+text+"\r\n")
        elif opt=="Error":
            print("<Error> "+text+"\r\n")
        else:
            print("Unsupported option "+opt+" for prinitng.")
     
            
    def __get_aux_data(self,attributes_array,row):
        returnobj={'features':{}}
        for att in attributes_array:
            if 'Level_IV' in att:
              returnobj['features']['Level_lV']=str(row[att])
            else:
              returnobj['features'][att]=str(row[att])            
            
        return json.dumps(returnobj)
     
        
            
            
            
    def __getGeoEntityID(GeoEntityIDKeys,row,type):
        """
        Purpose
        ----------
        This method will give ID of GeoEntity

        Parameters
        ----------
        ZoneIDKeys : Ordered Arrays for Key Parsing
        row : GeoJSON row for dataset extraction

        Returns
        -------
        zoneid : String contain ZoneID
        """
        if type=="Int":
            return str(int(row[GeoEntityIDKeys]))       
        else:
            return str(row[GeoEntityIDKeys])

        
    def __getGeom(wkt):
        """
        Purpose
        ----------
        This method will give ST_GeomFromGeoJSON fn string based on coordinate info from geojson row and can be used as geom in insert query.

        Parameters
        ----------
        wkt : WKT of Geometry

        Returns
        -------
        Geom WKT String
        """        
        return "ST_GeomFromText(\'"+str(wkt)+"\',4326)"
    
    #------------------------------------#
    # Main Methods for execution         #
    #------------------------------------#
    def main(self,config='config.json'):    
        self.__printMsg('Info',"====== GeoEntity ingestion  execution is started. ======")
        self.__printMsg('Info', "Config file is loading.")
        GeoEntityIngest.__config_file_path=config        
        # Configuration File Loading
        config_file=None
        try:
            config_file=open(GeoEntityIngest.__config_file_path)
        except:
            self.__printMsg('Error', "Sorry config file doesn't exist.")
            sys.exit()
        __Config = json.load(config_file)
        config_file.close()
        
        #Global Param Loading        
        host=__Config["global_param"]["database"]["host"]
        username=__Config["global_param"]["database"]["username"]
        password=__Config["global_param"]["database"]["password"]
        port=__Config["global_param"]["database"]["port"]
        db=__Config["global_param"]["database"]["db"]        
        geoentity_table=__Config["global_param"]["database"]["geoentity_table"]
        geoentity_source_table=__Config["global_param"]["database"]["geoentity_source_table"]
        geoentity_source_seq=__Config["global_param"]["database"]["geoentity_source_seq"]
        
        
        #Execution with Config Param Loading       
        __GeoEntityIngestConfig=__Config["config"]
        conn=None
        cur=None
        try:
            conn=psycopg2.connect(database=db, user=username, password=password, host=host, port=port)
            conn.autocommit = True
            cur= conn.cursor()
        except:
            self.__printMsg("Error"," DB Error, Please check DB Configuration once.")
            if conn is not None:
                cur.close()
                conn.close()
            self.__printMsg('Error',"====== GeoEntity ingestion execution is failed due to database connection. ======")
            sys.exit()   
        previous_parent_id=None
        for geoentity in __GeoEntityIngestConfig["geoentity_keys_to_process"]:
        
            #SourceInfo Loading
            source_name=__GeoEntityIngestConfig[geoentity]["geoentity_source"]["name"]
            source_publish_date_yyyymmdd=time.mktime(datetime.datetime.strptime(__GeoEntityIngestConfig[geoentity]["geoentity_source"]["publish_date_yyyymmdd"], "%Y%m%d").timetuple())
            source_project=__GeoEntityIngestConfig[geoentity]["geoentity_source"]["project"]
            source_provider=__GeoEntityIngestConfig[geoentity]["geoentity_source"]["provider"]
            source_category=__GeoEntityIngestConfig[geoentity]["geoentity_source"]["category"]
            source_aux=__GeoEntityIngestConfig[geoentity]["geoentity_source"]["aux_data"]
                       
            #ConfigInfo Loading
            config_geojsonfile_file_path=__GeoEntityIngestConfig[geoentity]["geoentity_config"]["geoJSON_file_config"]["file_path"]
            config_geojsonfile_parent_type=__GeoEntityIngestConfig[geoentity]["geoentity_config"]["geoJSON_file_config"]["parent_type"]
            config_geojsonfile_parent_geoent_source_id=__GeoEntityIngestConfig[geoentity]["geoentity_config"]["geoJSON_file_config"]["parent_geoentity_source_id"]
            config_geojsonfile_prefix_identifier=__GeoEntityIngestConfig[geoentity]["geoentity_config"]["geoJSON_file_config"]["prefix_identifier"]
            config_geojsonfile_infoattr_name=__GeoEntityIngestConfig[geoentity]["geoentity_config"]["geoJSON_file_config"]["geoJSON_info_attribute"]["name"]
            config_geojsonfile_infoattr_featureid=__GeoEntityIngestConfig[geoentity]["geoentity_config"]["geoJSON_file_config"]["geoJSON_info_attribute"]["feature_ID"]
            config_geojsonfile_infoattr_featureid_type="str"
            if "feature_ID_type" in __GeoEntityIngestConfig[geoentity]["geoentity_config"]["geoJSON_file_config"]["geoJSON_info_attribute"]:
                config_geojsonfile_infoattr_featureid_type=__GeoEntityIngestConfig[geoentity]["geoentity_config"]["geoJSON_file_config"]["geoJSON_info_attribute"]["feature_ID_type"]

            if (previous_parent_id is not None) and (config_geojsonfile_parent_geoent_source_id==-1):
                config_geojsonfile_parent_geoent_source_id=previous_parent_id
                self.__printMsg("Info"," Previous parent id:"+str(previous_parent_id)+" is set for geoentity: "+geoentity)
            elif (config_geojsonfile_parent_geoent_source_id==-1):
                self.__printMsg("Error","First element and incase of parent insertion failure parent_id can't be inherited. please check parent_geoentity_source_id configuration for geoentity: "+geoentity)
                sys.exit()
                
            #None Checking for Parameters
            config_var_list=[source_name,source_publish_date_yyyymmdd,source_project,source_provider,source_category,config_geojsonfile_file_path,config_geojsonfile_parent_type,config_geojsonfile_parent_geoent_source_id,config_geojsonfile_prefix_identifier,config_geojsonfile_infoattr_name,config_geojsonfile_infoattr_featureid]
            if (None in config_var_list) or ("" in config_var_list) :
                self.__printMsg('Error', "=====Configuration error, please see config once for "+geoentity+" ======")
                continue  
            else:
                config_var_list=None
            self.__printMsg('Info', geoentity+" Parameters(Global and Config) loaded successfully.")
            
            
            #Phase 1: Insertion Algo - Source Insertion
            self.__printMsg("Info", "Phase-1: GeoEntity Source Insertion Started.")
            geoentity_source_id=None
            source_insertion_query=None
            if not(source_aux=="NULL" or source_aux=="null" or source_aux=="Null" or source_aux==""):
                source_aux="'"+source_aux+"'"
            else:
                source_aux="NULL"
                
          #-----If duplicate exist then return id for phase-2 execution      
            try:
                cur.execute("SELECT setval('"+geoentity_source_seq+"', max(id)) from "+geoentity_source_table)
                if(config_geojsonfile_parent_geoent_source_id>0):
                    source_insertion_query="INSERT INTO "+geoentity_source_table+"(name, publish_date, project, provider, category,auxdata,parent_source_id) VALUES ('"+source_name+"', "+str(source_publish_date_yyyymmdd)+", '"+source_project+"', '"+source_provider+"', '"+source_category+"', "+source_aux+","+str(config_geojsonfile_parent_geoent_source_id)+") returning id;"
                else:
                    source_insertion_query="INSERT INTO "+geoentity_source_table+"(name, publish_date, project, provider, category,auxdata) VALUES ('"+source_name+"', "+str(source_publish_date_yyyymmdd)+", '"+source_project+"', '"+source_provider+"', '"+source_category+"', "+source_aux+") returning id;"
                self.__printMsg('Info', source_insertion_query)
                cur.execute(source_insertion_query)
                if(cur.rowcount<1): #0 row
                    self.__printMsg('Error', "=====(Phase1) Source insertion failed for "+geoentity+" ======")
                    continue
                else:                
                    geoentity_source_id=cur.fetchone()[0]
                    self.__printMsg('Info', " (Phase1) Source Insertion Completed Successfully.")    
            except psycopg2.Error as e:
                
                if "duplicate" in e.pgerror:
                    self.__printMsg('Error', "=====(Phase1) Already Source is existing for "+geoentity+" ======")
                    if __GeoEntityIngestConfig[geoentity]["geoentity_source"]["reprocess_flag"]:
                        source_id_query="select id from "+geoentity_source_table+" where name='"+source_name+"' and publish_date="+str(source_publish_date_yyyymmdd)+" and project='"+source_project+"' and provider='"+source_provider+"' and category='"+source_category+"'"
                        cur.execute(source_id_query)
                        geoentity_source_id=cur.fetchone()[0]
                    else:
                        sys.exit()
                else:
                    self.__printMsg('Error', " Phase1 Source Insertion for <"+geoentity+"> has been failed.")
                    previous_parent_id=None
                    continue
            self.__printMsg("Info", "Phase1: GeoEntity Source Processing Completed Successfully.")
            
            
            #Phase 2: Insertion Algo - GeoEntity Insertion
            self.__printMsg("Info", "Phase-2: GeoEntity Insertion Started.")
            previous_parent_id=geoentity_source_id
            gdf = None
            total_records=0
            processed_record=0
            failed_record=0
            try:
                gdf = gpd.read_file(config_geojsonfile_file_path)
                gdf.set_crs(epsg=4326, inplace=True, allow_override=True)
                self.__printMsg("Info"," Reading of "+geoentity+" geojson file has been completed.")
                total_records=gdf.shape[0]
                self.__printMsg("Info"," In "+geoentity+" total records for processing:"+str(total_records))
            except:
                self.__printMsg("Error", "Phase2 Sorry reading error for "+geoentity+" geojson file, please check the file once.")
                previous_parent_id=None
                continue
           
            for i,row in gdf.iterrows():
                # if i<2288559:
                #    continue   

                geoentity_id=config_geojsonfile_prefix_identifier+GeoEntityIngest.__getGeoEntityID(config_geojsonfile_infoattr_featureid,row,config_geojsonfile_infoattr_featureid_type)#--
                geoentity_name=row[config_geojsonfile_infoattr_name]#--  
                #print("geoname", geoentity_name, type(geoentity_name))   

                if(geoentity_name and (not isinstance(geoentity_name,float) or not math.isnan(geoentity_name))):
                    #print('i:',str(i),'geoentity_id:',geoentity_id,'geoentity_name:',geoentity_name)
                    geoentity_name=geoentity_name.replace("'","")
                else:
                    continue

                geoentity_geom=GeoEntityIngest.__getGeom(row.geometry.wkt)                
                geoentity_insertion_query=None
                if (config_geojsonfile_parent_geoent_source_id>0):
                    geoentity_insertion_query ="Insert into "+geoentity_table+"(geoentity_source_id, geoentity_id, name, geom,parent_geoentity_source_id) values ({0},'{1}','{2}',{3},{4})".format(geoentity_source_id,geoentity_id,geoentity_name,geoentity_geom,config_geojsonfile_parent_geoent_source_id)
                    if "geoJSON_aux_attributes" in __GeoEntityIngestConfig[geoentity]["geoentity_config"]["geoJSON_file_config"]:
                        geoentity_insertion_query ="Insert into "+geoentity_table+"(geoentity_source_id, geoentity_id, name, geom,parent_geoentity_source_id,auxdata) values ({0},'{1}','{2}',{3},{4},'{5}')".format(geoentity_source_id,geoentity_id,geoentity_name,geoentity_geom,config_geojsonfile_parent_geoent_source_id,self.__get_aux_data(__GeoEntityIngestConfig[geoentity]["geoentity_config"]["geoJSON_file_config"]["geoJSON_aux_attributes"],row))
                else:
                    geoentity_insertion_query ="Insert into "+geoentity_table+"(geoentity_source_id, geoentity_id, name, geom) values ({0},'{1}','{2}',{3})".format(geoentity_source_id,geoentity_id,geoentity_name,geoentity_geom)                            
                    if "geoJSON_aux_attributes" in __GeoEntityIngestConfig[geoentity]["geoentity_config"]["geoJSON_file_config"]:
                        geoentity_insertion_query ="Insert into "+geoentity_table+"(geoentity_source_id, geoentity_id, name, geom,auxdata) values ({0},'{1}','{2}',{3},'{4}')".format(geoentity_source_id,geoentity_id,geoentity_name,geoentity_geom,self.__get_aux_data(__GeoEntityIngestConfig[geoentity]["geoentity_config"]["geoJSON_file_config"]["geoJSON_aux_attributes"],row))                            
                try:
                    cur.execute(geoentity_insertion_query)
                    if cur.rowcount == 1:
                        processed_record=processed_record+1        
                    else:
                        failed_record=failed_record+1
                except psycopg2.Error as e:
                    if "duplicate" in e.pgerror:
                        if __GeoEntityIngestConfig[geoentity]["geoentity_source"]["reprocess_flag"]:
                            processed_record=processed_record+1
                            continue
                        else:
                            sys.exit()
                    else:
                        self.__printMsg("Error", e.pgerror)
                        
            self.__printMsg("Info"," Phase2 GeoEntity Insertion: Successfully processed records:"+str(processed_record))
            self.__printMsg("Info"," Phase2 GeoEntity Insertion: Failed Records:"+str(failed_record)+" \n")
            self.__printMsg("Info", "Phase2: GeoEntity Insertion Successfully Completed.")
                
            
            #Phase3: Spaitail Join 
            #Phase3: Parent Condition Checking
            if (config_geojsonfile_parent_geoent_source_id!=0):
                self.__printMsg("Info"," Phase3: Spatial join statred.")
                #geoentity_parent_update_query="UPDATE "+geoentity_table+" SET geoentity_id=CONCAT(parent.geoentity_id,"+geoentity_table+".geoentity_id), parent_id=parent.geoentity_id, parent_name=parent.name FROM (SELECT geoentity_id, name,ST_Buffer(geom::geography,2000)::geometry as geom FROM "+geoentity_table+" where geoentity_source_id="+str(config_geojsonfile_parent_geoent_source_id)+") parent WHERE geoentity_source_id="+str(geoentity_source_id)+" and geoentity.parent_geoentity_source_id="+str(config_geojsonfile_parent_geoent_source_id)+" and ST_Contains(parent.geom,geoentity.geom);"
                geoentity_parent_update_query="UPDATE "+geoentity_table+" AS child SET geoentity_id = CONCAT(parent.geoentity_id, child.geoentity_id), parent_id = parent.geoentity_id, parent_name = parent.name, parent_geoentity_source_id = parent.geoentity_source_id FROM (SELECT geoentity_source_id, geoentity_id, name, geom FROM  "+geoentity_table+" WHERE geoentity_source_id = "+str(config_geojsonfile_parent_geoent_source_id)+") AS parent WHERE child.geoentity_source_id = "+str(geoentity_source_id)+" AND ST_Intersects(parent.geom, child.geom) AND ST_Contains(parent.geom, ST_Centroid(child.geom))";                
                #If parent name is set then update query will not be executed
                update_test_query="SELECT COUNT(*) FROM geoentity where geoentity_source_id = "+str(geoentity_source_id)+" and parent_name is not NULL; "
                cur.execute(update_test_query)
                noof_updated_rows=cur.fetchone()[0]
                if noof_updated_rows<1:
                    command="psql -h "+host+" -U "+username+" -d "+db+" -p "+str(port)+" -c \""+geoentity_parent_update_query+"\""
                    if(__GeoEntityIngestConfig[geoentity]["geoentity_config"]["geoJSON_file_config"]["spatailjoin_flag"]):
                        os.system(command)
                    else:
                        self.__printMsg("Info","Updte Query is: "+geoentity_parent_update_query)
                    self.__printMsg("Info", " Phase3: Spatail join is performed for "+ geoentity)
                else:
                    self.__printMsg(geoentity_parent_update_query)                 
                self.__printMsg("Info", " Phase3 Process Completed for "+ geoentity) 
                
               
            #Local Variable Reset to None
            source_name=None
            source_publish_date_yyyymmdd=None
            source_project=None
            source_provider=None
            source_category=None
            source_aux=None            
            config_geojsonfile_file_path=None
            config_geojsonfile_parent_type=None
            config_geojsonfile_parent_geoent_source_id=None
            config_geojsonfile_prefix_identifier=None
            config_geojsonfile_infoattr_name=None
            config_geojsonfile_infoattr_featureid=None
        if conn is not None:    
            cur.close()
            conn.close()
        self.__printMsg('Info',"====== GeoEntity Execution Completed and and All DB Connections are Closed. ======")
        sys.exit()   
        

if __name__ == "__main__":
    MainObj=GeoEntityIngest()    
    MainObj.main(sys.argv[1])    
