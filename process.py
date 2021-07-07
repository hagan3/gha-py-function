import os, sys, logging
import argparse
import pandas as pd
import numpy as np
import math

# running locally
if os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir)) not in sys.path:
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir)))

import shared.utils as utils
import shared.table_schema as table_schema

etl_configs = utils.load_config(os.path.join(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'configs'), 'default.yml'), type='yaml')
etl_configs = {**etl_configs, **utils.him_configs}

class Pipeline:
    def __init__(self):
        self._jobs = {
            "material_mapping": MaterialMappingETL,
            'tank_metadata': TankMetadataETL,
            'tag_lookup': TagLookupETL,
            'tank_darwin': TankDarwinETL,
            'tank_shutdown': TankShutdownETL,
            'generic' : GenericETL,
        }

    def get_job(self, job_name, **kwargs):
        job = self._jobs.get(job_name)
        if not job:
            raise ValueError(f'No job found for name={job_name}')

        if 'params' in kwargs and kwargs['params']:
            return job(**kwargs)        
        else:
            return job()

class ETL:
    def __init__(self):
        self.container = 'data'
        self.engine = utils.engine
        self.db_schema = utils.db_schema

    def loadSource(self, container, blob_path, sheet_names=None):
        return utils.loadBlobToDf(container, blob_path, use_xlsx_sheet_name=True, sheet_names=sheet_names)

    def create_table_class(self, table_name):
        table_dict = table_schema.create_supported_table_schemas(utils.db_schema, table_name)        
        table_classes = utils.create_tables(table_dict, utils.engine)        
        return table_classes[table_name][0]

    def drop_table(self, table_name):
        self.engine.execute(f'drop table if exists {self.db_schema}.{table_name};')

    def recreate_table(self, table_name):
        self.drop_table(table_name)
        return self.create_table_class(table_name)
                
    def loadDFtoSql(self, df, table_name):
        self.drop_table(table_name)
        table_class = self.create_table_class(table_name)
        utils.load_df_to_table_class(df, table_class, truncate=False, batch_size=len(df.index))        

    def loadDFtoSql_table_class(self, df, table_class):
        utils.load_df_to_table_class(df, table_class, truncate=False, batch_size=len(df.index))        

    def retrieve_managed_table(self, tn_str):
        return table_schema.HimManagedSqlTables(tn_str).value

    def create_sql_view(self, table_name, view_select_clause):
        utils.logging.info(f'Recreate view.')
        utils.create_view(f'{self.db_schema}.{table_name}_view', view_select_clause)        

class MaterialMappingETL(ETL):
    def __init__(self):
        super().__init__()
        self.blob_path = '01_raw/Static/adf/MaterialMapping.xlsx'
        self.sheet_names = ['pump_grade_mapping', 'tank_grade_mapping','crude_grades']         

    def perform(self):
        logging.info(f'executing job...')
        df_dict = self.loadSource(self.container, self.blob_path)
        for sheet_name, df in df_dict.items():
            logging.info(f'loaded source data, sheet_name={sheet_name}, total_rows={len(df.index)}')
            if sheet_name.endswith('pump_grade_mapping'):
                df.columns = df.iloc[0].tolist()
                df = df.iloc[1:]
                df = df.rename(columns={'Pump Tag': 'material_tag', 'Grade': 'grade', 'heavy/sour/light': 'density_sulfur_content', 'KM Product No':'km_product_no' })
                
                df = df.astype(object).where(pd.notnull(df), None)
                
                self.loadDFtoSql(df, 'material_grade_mapping')

        logging.info(f'job successfully completed!')


class TankMetadataETL(ETL):
    def __init__(self):
        super().__init__()
        self.blob_path = '01_raw/Static/adf/ComprehensiveCrudeInventoryLocations.xlsx'
        self.sheet_names = ['CrudeTanks_Upgrader', 'CrudeTanks_Edmonton','CrudeTanks_Montreal','CrudeTanks_Sarnia', 'CrudeTanks_Denver']         

        # column name mapping from spreadsheet to sql table
        self.column_map = {
            'tankID': 'tank_id',
            'tank_id_systems':	'tank_id_systems',
            'service_code': 'service_code',
            'product': 'product',
            'tank_farm_area': 'tank_farm_area',
            'sub_tank_farm_area': 'sub_tank_farm_area',
            'tank_height_ft': 'tank_height_ft',
            'tank_diameter_ft': 'tank_diameter_ft',
            'tank_bottom_l1u_bbl':	'tank_bottom_l1u_bbl',
            'tank_bottom_l1u_ft': 'tank_bottom_l1u_ft',
            'tank_bottom_l1m_bbl': 'tank_bottom_l1m_bbl',
            'tank_bottom_l1m_ft': 'tank_bottom_l1m_ft',
            'safety_stock': 'safety_stock',
            'tank_bottom_l1oper_bbl': 'tank_bottom_l1oper_bbl',
            'tank_bottom_l1oper_ft': 'tank_bottom_l1oper_ft',
            'tank_bottom_description': 'tank_bottom_description',
            'll_alarm_bbl': 'll_alarm_bbl',
            'll_alarm_ft': 'll_alarm_ft',
            'l_alarm_bbl': 'l_alarm_bbl',
            'l_alarm_ft': 'l_alarm_ft',
            'l2_operational_safety_stock': 'l2_operational_safety_stock',
            'operational_min_l2_bbl': 'operational_min_l2_bbl',
            'ullage_bbl': 'ullage_bbl',
            'L3_discretionary_bbl': 'L3_discretionary_bbl',
            'operating_capacity_bbl': 'operating_capacity_bbl', 
            'h_alarm_bbl': 'h_alarm_bbl',
            'h_alarm_ft': 'h_alarm_ft',
            'hh_alarm_bbl':	'hh_alarm_bbl',
            'hh_alarm_ft': 'hh_alarm_ft',
            'max_shell_capacity': 'max_shell_capacity',  
            'tank_maximum_description':	'tank_maximum_description',
            'roof_type': 'roof_type',
            'oper_trading_both': 'purpose',
        }

    def fix_tank_id(self, x):
        lst = x.split('-', 1)
        return f'{lst[0]}-{lst[1][0:2]}{lst[1][2:].zfill(3)}'

    def filter_tank_id(self, v):
        try:
            lst = v.split('-', 1)
            if len(lst) == 2 and lst[1][0:2].lower() == 'tk':
                return True
            else:
                return False
        except:
            return False

    def perform(self):
        logging.info(f'executing job for {self.blob_path} ...')
        df_dict = self.loadSource(self.container, self.blob_path, sheet_names=self.sheet_names)
        table_class = self.recreate_table(table_schema.HimManagedSqlTables.TANK_METADATA.value)
        for sheet_name, df in df_dict.items():
            df.columns = [x.strip() for x in df.iloc[1].tolist()]
            df = df.iloc[3:]
            df = df.rename(columns=self.column_map)
            df = df[~df['tank_id'].isnull()]
            df = df[df['tank_id'].apply(self.filter_tank_id)]            
            df.loc[:, 'tank_id'] = df['tank_id'].apply(self.fix_tank_id)
            df['tank_bottom_l1u_bbl'] = pd.to_numeric(df['tank_bottom_l1u_bbl'], errors = 'coerce')
            df = df.apply(lambda x: x.astype(str).str.strip().replace({'nan': np.nan,'':np.nan}))
            df = df.astype(object).where(pd.notnull(df), None)
            logging.info(f'loaded source data, sheet_name={sheet_name}, total_rows={len(df.index)}')
            self.loadDFtoSql_table_class(df, table_class)

        logging.info(f'job successfully completed!')

class TagLookupETL(ETL):
    def __init__(self):
        super().__init__()
        self.blob_path = '01_raw/Static/adf/tagConversionLookup.xlsx'
        self.sheet_names = ['Upgrader', 'Edmonton','Montreal','Sarnia', 'Denver']         

        # column name mapping from spreadsheet to sql table
        self.column_map = {
            'TagName': 'tag_name',
            'TagUnit': 'tag_unit',
            'TagDescription': 'tag_description',
            'TagCaptureType': 'tag_capture_type',
            'TagUsedCalculation': 'tag_used_calculation',
            'Location': 'location',
            'TankSource': 'tank_source',
            'CommonName': 'common_name',
            'dtype': 'dtype',
            'CommonUnit': 'common_unit',
            'CommonUnitConversion': 'common_unit_conversion',	
            'CommonDescription': 'common_description',
            'FieldMin': 'field_min',
            'FieldMax': 'field_max',
            'LevelToVolume': 'level_to_volume',	
            'LL': 'll',
            'L': 'l',
            'H': 'h',
            'HH': 'hh',
            'InformationSources': 'information_sources',	
            'Comment': 'comment'
        }

    def adjust_tank_source(self, row):
        if not pd.isnull(row['tank_source']):
            return row['tank_source']
        elif pd.isnull(row['tank_source']) and row['common_name'] == 'DateTime':
            return 'DateTime'
        else:
            msg = f'Invalid row found in tanklookup! row={row}'
            logging.exception(msg)
            raise Exception(msg)

    def perform(self):
        logging.info(f'executing job for {self.blob_path} ...')
        df_dict = self.loadSource(self.container, self.blob_path, sheet_names=self.sheet_names)
        table_class = self.recreate_table(table_schema.HimManagedSqlTables.TAG_LOOKUP.value)
        for sheet_name, df in df_dict.items():
            logging.info(f'working on sheet_name={sheet_name}')
            df.columns = df.iloc[0].str.strip().tolist()
            df = df.iloc[1:]
            df = df.rename(columns=self.column_map)
            for e in ['tag_name', 'tag_capture_type', 'location', 'tank_source', 'common_name']:
                if e in df.columns: df[e] = df[e].str.strip()
            df = df[~df['tag_name'].isnull()]
            # filtering out tags without tank source value
            df = df.loc[~df['common_name'].isnull()]
            df['tank_source'] = df.apply(self.adjust_tank_source, axis=1)
            df['tank_id'] = df['location'] + '-' + df['tank_source']
            df = df.astype(object).where(pd.notnull(df), None)
            self.loadDFtoSql_table_class(df, table_class)
            logging.info(f'loaded source data, sheet_name={sheet_name}, total_rows={len(df.index)}')

        logging.info(f'job successfully completed!')

class GenericETL(ETL):
    def __init__(self, params=None):
        super().__init__()
        self.blob_path = params['blob_path']
        self.sheet_names = None
        self.sql_table_name = params['sql_table_name']
        self.src2sql_col_map = params['src2sql_col_map']
        self.sql_view_select_clause = params['sql_view_select_clause']


    def perform(self):
        logging.info(f'executing job for: blob_path={self.blob_path}, sql_table_name={self.sql_table_name}, src2sql_col_map={self.src2sql_col_map}, sql_view_select_clause={self.sql_view_select_clause}')
        df_dict = self.loadSource(self.container, self.blob_path, sheet_names=self.sheet_names)
        table_class = self.recreate_table(self.retrieve_managed_table(self.sql_table_name))
        for sheet_name, df in df_dict.items():
            df.columns = df.iloc[0].tolist()
            df = df.iloc[1:]
            df = df.rename(columns=self.src2sql_col_map)

            if df.columns.str.contains('datetime', case=False).any():
                df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')

            df = df.astype(object).where(pd.notnull(df), None)
            logging.info(f'loaded source data, sheet_name={sheet_name}, total_rows={len(df.index)}')
            self.loadDFtoSql_table_class(df, table_class)

        self.create_sql_view(self.sql_table_name, self.sql_view_select_clause)
        logging.info(f'job successfully completed!')        

class TankDarwinETL(ETL):
    def __init__(self):
        super().__init__()
        self.blob_path = '01_raw/Static/adf/Levels_Darwin.xlsx'
        self.sheet_names = ['Upgrader', 'Edmonton','Montreal','Sarnia', 'Denver']         

        # column name mapping from spreadsheet to sql table
        self.column_map = {
            'tankID':'tank_id',
            'grade':'grade',
            'marker':'marker',
            'operational_min_darwin':'operational_min_darwin',
            'tradable_tank':'tradable_tank',
            'fungible_tank':'fungible_tank',
            'max_equity_use':'max_equity_use',
            'utilization_max':'utilization_max',
            'trading_flexibility':'trading_flexibility',
            'equity_recommendation':'equity_recommendation_percentage'
        }

    def _df_adjustments(self, df):
        for col in ['tradable_tank','fungible_tank']:
            df[col] = df[col].apply(lambda x: None if pd.isnull(x)  else (False if x == 'N' else True))

    def perform(self):
        logging.info(f'executing job for {self.blob_path}')
        df_dict = self.loadSource(self.container, self.blob_path, sheet_names=self.sheet_names)
        table_class = self.recreate_table(table_schema.HimManagedSqlTables.TANK_DARWIN.value)
        for sheet_name, df in df_dict.items():
            df.columns = df.iloc[0].tolist()
            df = df.iloc[1:]
            df = df.rename(columns=self.column_map)
            self._df_adjustments(df)
            df = df[~df['tank_id'].isnull()]
            df = df.astype(object).where(pd.notnull(df), None)
            logging.info(f'loaded source data, sheet_name={sheet_name}, total_rows={len(df.index)}')
            self.loadDFtoSql_table_class(df, table_class)

        logging.info(f'job successfully completed!')        

class TankShutdownETL(ETL):
    def __init__(self):
        super().__init__()
        self.blob_path = '01_raw/Static/adf/tank_shutdown.xlsx'
        self.sheet_names = ['Upgrader', 'Edmonton','Montreal','Sarnia', 'Denver']         

        # column name mapping from spreadsheet to sql table
        self.column_map = {
            'TankID':'tank_id',
            'Start_date':'start_date',
            'End_date':'end_date'
        }

    def perform(self):
        logging.info(f'executing job for {self.blob_path}')
        df_dict = self.loadSource(self.container, self.blob_path, sheet_names=self.sheet_names)
        table_class = self.recreate_table(table_schema.HimManagedSqlTables.TANK_SHUTDOWN.value)
        for sheet_name, df in df_dict.items():
            df.columns = df.iloc[0].tolist()
            df = df.iloc[1:]
            df = df.rename(columns=self.column_map)

            df = df[~df['start_date'].isnull()]

            #if sheet_name == 'Edmonton':
            #    df['tank_id'] = df['tank_id'].apply(lambda x: x if 'Edmonton' in x else f'Edmonton-{x}')
            #elif sheet_name == 'Montreal':
            #    df['tank_id'] = df['tank_id'].apply(lambda x: x if 'TK' in x else None)

            #df = df[~df['tank_id'].isnull()]

            df = df.astype(object).where(pd.notnull(df), None)
            logging.info(f'loaded source data, sheet_name={sheet_name}, total_rows={len(df.index)}')
            self.loadDFtoSql_table_class(df, table_class)

        logging.info(f'job successfully completed!')        

def get_job_params(job_param_name):
    if job_param_name == 'load_edmonton_kpi_dfs_forward':
        return load_edmonton_kpi_dfs_forward()
    elif job_param_name == 'load_edmonton_kpi_tutt_forward':
        return load_edmonton_kpi_tutt_forward()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--job_name', type=str,  help='job name', required=True)
    parser.add_argument('--job_params', type=str,  help='job params', required=False)
    parser.add_argument('--env', type=str, help='execution environment', required=False)    
    args = parser.parse_args()

    utils.updateEnviornment(args['env'])

    #print({'params': etl_configs.get(args.job_params, {})})

    job_name = args.job_name
    job = Pipeline().get_job(job_name, **{'params': etl_configs.get(args.job_params, {})})
    job.perform()