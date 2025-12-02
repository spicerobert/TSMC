import pygsheets as pg
import pandas as pd
import os
import configparser
import json
import tempfile

class DataValidationError(Exception):
    """自訂例外類別，用於處理資料驗證錯誤"""
    pass

class GSheetReader:
    def __init__(self):
        """初始化 GSheetReader，載入設定並連接 Google Sheets API"""
        self._load_config()
        self._authorize_gsheets()
        self._load_budget_data()

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

    def _load_budget_data(self):
        """從 Google Sheet 讀取預算資料"""
        try:
            worksheet1_title = 'CF人力預算(第一版)'
            worksheet2_title = '人力預算(第二版)'

            worksheet1 = self.data_wb.worksheet_by_title(worksheet1_title)
            df1 = worksheet1.get_as_df()
            print(f"成功讀取 '{worksheet1_title}' 工作表，共 {len(df1)} 筆資料")
            print(f"'{worksheet1_title}' 工作表欄位: {df1.columns.tolist()}")
            print("\n資料預覽:")
            print(df1.head())

            print("-" * 50)

            worksheet2 = self.data_wb.worksheet_by_title(worksheet2_title)
            df2 = worksheet2.get_as_df()
            print(f"成功讀取 '{worksheet2_title}' 工作表，共 {len(df2)} 筆資料")
            print(f"'{worksheet2_title}' 工作表欄位: {df2.columns.tolist()}")
            print("\n資料預覽:")
            print(df2.head())
            
        except Exception as e:
            raise DataValidationError(f"從 Google Sheet 讀取預算資料時發生錯誤：{e}")

if __name__ == '__main__':
    try:
        gsheet_reader = GSheetReader()
        print("\nGoogle Sheet 預算資料讀取成功！")
    except (DataValidationError, ConnectionError) as e:
        print(f"發生錯誤：{e}")
