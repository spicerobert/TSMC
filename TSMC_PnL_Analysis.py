import pygsheets as pg
import pandas as pd
import os
import configparser
import json
import tempfile
import numpy as np

class DataValidationError(Exception):
    """自訂例外類別，用於處理資料驗證錯誤"""
    pass

class GSheetReader:
    def __init__(self):
        """初始化 GSheetReader，載入設定並連接 Google Sheets API"""
        self._load_config()
        self._authorize_gsheets()
        self._load_GS_data()

    def _load_config(self):
        """從 config.ini 載入設定"""
        config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.ini')
        if not os.path.exists(config_path):
            raise DataValidationError("設定檔案 config.ini 未找到")
        
        self.config = configparser.ConfigParser()
        self.config.read(config_path, encoding='utf-8')

        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                content = f.read()
            json_str_match = content.split('json_content = """')
            if len(json_str_match) > 1:
                json_str = json_str_match[1].split('"""')[0]
                self.service_account_json = json.loads(json_str)
            else:
                raise DataValidationError("在 config.ini 中找不到 json_content")
        except (json.JSONDecodeError, IndexError) as e:
            raise DataValidationError(f"解析 config.ini 中的 service_account_json 失敗: {e}")

    def _authorize_gsheets(self):
        """使用服務帳號金鑰授權並連接 Google Sheets"""
        try:
            with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.json', encoding='utf-8') as temp_key_file:
                json.dump(self.service_account_json, temp_key_file)
                temp_key_path = temp_key_file.name
            
            self.gc = pg.authorize(service_file=temp_key_path)
            os.unlink(temp_key_path)
            self.data_wb = self.gc.open_by_url(self.config['GOOGLE_SHEETS']['data_sheet_url'])
        except Exception as e:
            raise ConnectionError(f"Google Sheets API 初始化失敗: {e}")

    def _load_GS_data(self):
        """從 Google Sheet 讀取 TSMC_PnL 資料"""
        try:
            self.raw_true_pnl_data = self.data_wb.worksheet_by_title('Raw_TruePnL').get_as_df()
            self.ref_department_data = self.data_wb.worksheet_by_title('Ref_Department').get_as_df()
            self.ref_accounts_data = self.data_wb.worksheet_by_title('Ref_Accounts').get_as_df()
            
            print(f"成功讀取 'Raw_TruePnL' 工作表，共 {len(self.raw_true_pnl_data)} 筆資料")
            print(f" 'Raw_TruePnL' 工作表欄位: {self.raw_true_pnl_data.columns.tolist()}")
            print("\n'Raw_TruePnL' 工作表資料:")
            print(self.raw_true_pnl_data)
            # print(f"\n成功讀取 'Ref_Department' 工作表，共 {len(self.ref_department_data)} 筆資料")
            # print(f" 'Ref_Department' 工作表欄位: {self.ref_department_data.columns.tolist()}")
            # print(f"成功讀取 'Ref_Accounts' 工作表，共 {len(self.ref_accounts_data)} 筆資料")
            # print(f" 'Ref_Accounts' 工作表欄位: {self.ref_accounts_data.columns.tolist()}")
            
        except Exception as e:
            raise DataValidationError(f"從 Google Sheet 讀取 TSMC_PnL 資料時發生錯誤：{e}")

if __name__ == '__main__':
    try:
        gsheet_reader = GSheetReader()
        print("\nTSMC_PnL 資料讀取成功！")
    except (DataValidationError, ConnectionError) as e:
        print(f"發生錯誤：{e}")
