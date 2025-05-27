import sqlite3
import logging
import os
from datetime import datetime
from typing import List, Dict, Optional, Any # Added Optional, Any

# 配置日誌
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DAL:
    """
    數據訪問層 (Data Access Layer) 類，用於處理與SQLite資料庫的交互。
    """
    def __init__(self, db_path: str = None):
        if db_path is None:
            self.db_path = os.path.join(os.path.dirname(__file__), 'app_data.db')
        else:
            self.db_path = db_path
        
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"DAL initialized with database path: {self.db_path}")
        self.initialize_database()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row # Access columns by name
        return conn

    def initialize_database(self):
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                # internal_file_key 應該是唯一的，這樣才能可靠地引用它。
                # 如果 internal_file_key + related_entity 需要唯一，則應創建複合唯一索引。
                # 目前按 internal_file_key 全局唯一。
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS imported_files (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ui_file_type TEXT NOT NULL,
                    internal_file_key TEXT NOT NULL UNIQUE, 
                    file_path TEXT NOT NULL,
                    original_file_name TEXT,
                    import_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    status TEXT DEFAULT 'imported',
                    related_entity TEXT NOT NULL 
                )
                """)
                self.logger.info("'imported_files' table ensured.")
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS app_config (
                    config_key TEXT PRIMARY KEY,
                    config_value TEXT,
                    description TEXT
                )
                """)
                self.logger.info("'app_config' table ensured.")
                conn.commit()
            self.logger.info("Database initialized successfully.")
        except sqlite3.Error as e:
            self.logger.error(f"Error initializing database: {e}", exc_info=True)
            raise

    def add_imported_file(self, ui_file_type: str, internal_file_key: str, file_path: str, 
                            original_file_name: str, related_entity: str) -> Optional[int]:
        """
        添加一個導入文件記錄。如果具有相同 internal_file_key 的記錄已存在，則更新它。
        返回插入或更新記錄的 ID。
        """
        # SQLite's INSERT OR REPLACE 會刪除舊行並插入新行，導致 ID 變化。
        # 使用 INSERT ... ON CONFLICT DO UPDATE 可以保留ID（如果主鍵不是internal_file_key）
        # 或者先查後插/更。由於 internal_file_key 是 UNIQUE 但不是主鍵，我們可以用它來做 UPSERT。
        sql_upsert = """
        INSERT INTO imported_files 
        (ui_file_type, internal_file_key, file_path, original_file_name, related_entity, import_timestamp, status) 
        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, 'imported')
        ON CONFLICT(internal_file_key) DO UPDATE SET
            ui_file_type = excluded.ui_file_type,
            file_path = excluded.file_path,
            original_file_name = excluded.original_file_name,
            related_entity = excluded.related_entity,
            import_timestamp = CURRENT_TIMESTAMP,
            status = 'imported';
        """
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql_upsert, (ui_file_type, internal_file_key, file_path, original_file_name, related_entity))
                conn.commit()
                
                # 获取 upserted row的id
                if cursor.lastrowid == 0: # 如果是UPDATE，lastrowid可能是0或者不變
                    cursor.execute("SELECT id FROM imported_files WHERE internal_file_key = ?", (internal_file_key,))
                    row = cursor.fetchone()
                    if row:
                        self.logger.info(f"Upserted file (updated or inserted): {internal_file_key}, ID: {row['id']}")
                        return row['id']
                    else: # 理論上不可能發生，因為 upsert 保證了行的存在
                        self.logger.error(f"Failed to retrieve ID after upsert for key {internal_file_key}")
                        return None
                else: # 如果是INSERT
                    self.logger.info(f"Upserted file (inserted): {internal_file_key}, ID: {cursor.lastrowid}")
                    return cursor.lastrowid
        except sqlite3.Error as e:
            self.logger.error(f"Error adding/updating imported file with key '{internal_file_key}': {e}", exc_info=True)
            return None

    def remove_imported_file(self, internal_file_key: str, related_entity: str) -> bool:
        """按 internal_file_key 和 related_entity 刪除導入的文件記錄。"""
        # 注意：如果 internal_file_key 是全局唯一的，related_entity 可能不是嚴格必需的
        # 但如果一個 internal_file_key 可以用於多個 entity (儘管當前表結構不允許)，則需要它。
        # 根據當前表結構 (internal_file_key UNIQUE)，related_entity 在此處主要用於雙重確認。
        sql = "DELETE FROM imported_files WHERE internal_file_key = ? AND related_entity = ?"
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql, (internal_file_key, related_entity))
                conn.commit()
                if cursor.rowcount > 0:
                    self.logger.info(f"Removed file: key='{internal_file_key}', entity='{related_entity}'")
                    return True
                self.logger.warning(f"No file found to remove with key='{internal_file_key}', entity='{related_entity}'")
                return False
        except sqlite3.Error as e:
            self.logger.error(f"Error removing file with key='{internal_file_key}', entity='{related_entity}': {e}", exc_info=True)
            return False

    def get_imported_file_path(self, internal_file_key: str, related_entity: str) -> Optional[str]:
        """按 internal_file_key 和 related_entity 獲取文件路徑。"""
        sql = "SELECT file_path FROM imported_files WHERE internal_file_key = ? AND related_entity = ?"
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql, (internal_file_key, related_entity))
                row = cursor.fetchone()
                if row:
                    return row["file_path"]
                return None
        except sqlite3.Error as e:
            self.logger.error(f"Error getting file path for key='{internal_file_key}', entity='{related_entity}': {e}", exc_info=True)
            return None

    def get_imported_files_by_entity(self, related_entity: str) -> List[Dict[str, Any]]:
        """按 related_entity 獲取所有相關導入文件的記錄列表。"""
        sql = "SELECT * FROM imported_files WHERE related_entity = ?"
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql, (related_entity,))
                rows = cursor.fetchall()
                return [dict(row) for row in rows] # 將 sqlite3.Row 對象轉換為字典列表
        except sqlite3.Error as e:
            self.logger.error(f"Error getting files for entity='{related_entity}': {e}", exc_info=True)
            return []

    def clear_imported_files_by_entity(self, related_entity: str) -> bool:
        """按 related_entity 清除所有相關的導入文件記錄。"""
        sql = "DELETE FROM imported_files WHERE related_entity = ?"
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql, (related_entity,))
                conn.commit()
                self.logger.info(f"Cleared all files for entity='{related_entity}'. Rows affected: {cursor.rowcount}")
                return True # 即使沒有行被刪除，操作本身也是成功的
        except sqlite3.Error as e:
            self.logger.error(f"Error clearing files for entity='{related_entity}': {e}", exc_info=True)
            return False

    def update_file_status(self, internal_file_key: str, related_entity: str, new_status: str) -> bool:
        """更新指定文件的狀態。"""
        sql = "UPDATE imported_files SET status = ?, import_timestamp = CURRENT_TIMESTAMP WHERE internal_file_key = ? AND related_entity = ?"
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql, (new_status, internal_file_key, related_entity))
                conn.commit()
                if cursor.rowcount > 0:
                    self.logger.info(f"Updated status for file key='{internal_file_key}', entity='{related_entity}' to '{new_status}'")
                    return True
                self.logger.warning(f"No file found to update status for key='{internal_file_key}', entity='{related_entity}'")
                return False
        except sqlite3.Error as e:
            self.logger.error(f"Error updating status for file key='{internal_file_key}', entity='{related_entity}': {e}", exc_info=True)
            return False

    def get_config_value(self, config_key: str, default_value: Optional[str] = None) -> Optional[str]:
        sql = "SELECT config_value FROM app_config WHERE config_key = ?"
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql, (config_key,))
                row = cursor.fetchone()
                return row["config_value"] if row else default_value
        except sqlite3.Error as e:
            self.logger.error(f"Error getting config value for key '{config_key}': {e}", exc_info=True)
            return default_value

    def set_config_value(self, config_key: str, config_value: str, description: Optional[str] = None) -> bool:
        sql = "INSERT OR REPLACE INTO app_config (config_key, config_value, description) VALUES (?, ?, ?)"
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql, (config_key, config_value, description))
                conn.commit()
                self.logger.info(f"Set config: {config_key} = {config_value}")
                return True
        except sqlite3.Error as e:
            self.logger.error(f"Error setting config value for key '{config_key}': {e}", exc_info=True)
            return False


if __name__ == '__main__':
    db_file = 'test_dal_operations.db'
    if os.path.exists(db_file):
        os.remove(db_file)
    dal = DAL(db_path=db_file)
    logging.info(f"--- Testing DAL operations with database: {dal.db_path} ---")

    # 測試 Config
    dal.set_config_value("user_name", "test_user", "Last logged in user")
    dal.set_config_value("theme", "dark", "UI theme preference")
    assert dal.get_config_value("user_name") == "test_user"
    assert dal.get_config_value("theme") == "dark"
    assert dal.get_config_value("non_existent_key", "default") == "default"
    dal.set_config_value("theme", "light") # Test update
    assert dal.get_config_value("theme") == "light"
    logging.info("Config operations tested successfully.")

    # 測試 Files
    file_id1 = dal.add_imported_file("POPR", "raw_data_main", "/path/to/popr.csv", "popr_202312.csv", "MOBA_PO")
    assert file_id1 is not None
    file_id2 = dal.add_imported_file("Closing List", "closing_list_main", "/path/to/closing.xlsx", "closing_202312.xlsx", "MOBA_PO")
    assert file_id2 is not None
    file_id3 = dal.add_imported_file("SPX PO", "spx_po_data", "/path/to/spx_po.csv", "spx_po_202401.csv", "SPX")
    assert file_id3 is not None
    
    # 測試 get_imported_file_path
    path1 = dal.get_imported_file_path("raw_data_main", "MOBA_PO")
    assert path1 == "/path/to/popr.csv"
    path_non_existent = dal.get_imported_file_path("non_existent_key", "MOBA_PO")
    assert path_non_existent is None
    logging.info("get_imported_file_path tested.")

    # 測試 UPSERT (更新現有 internal_file_key)
    file_id1_updated = dal.add_imported_file("POPR Updated", "raw_data_main", "/new_path/to/popr.csv", "popr_202312_v2.csv", "MOBA_PO")
    assert file_id1_updated == file_id1 # ID 應該保持不變或為更新後的ID
    updated_path = dal.get_imported_file_path("raw_data_main", "MOBA_PO")
    assert updated_path == "/new_path/to/popr.csv"
    logging.info("File UPSERT (update) tested.")
    
    # 測試 get_imported_files_by_entity
    moba_po_files = dal.get_imported_files_by_entity("MOBA_PO")
    assert len(moba_po_files) == 2
    assert any(f['internal_file_key'] == 'raw_data_main' and f['file_path'] == '/new_path/to/popr.csv' for f in moba_po_files)
    spx_files = dal.get_imported_files_by_entity("SPX")
    assert len(spx_files) == 1
    assert spx_files[0]['internal_file_key'] == 'spx_po_data'
    logging.info("get_imported_files_by_entity tested.")

    # 測試 update_file_status
    assert dal.update_file_status("raw_data_main", "MOBA_PO", "processed") == True
    moba_po_files_after_status_update = dal.get_imported_files_by_entity("MOBA_PO")
    raw_data_file_updated = next(f for f in moba_po_files_after_status_update if f['internal_file_key'] == 'raw_data_main')
    assert raw_data_file_updated['status'] == 'processed'
    assert dal.update_file_status("non_existent_key", "MOBA_PO", "processed") == False
    logging.info("update_file_status tested.")

    # 測試 remove_imported_file
    assert dal.remove_imported_file("closing_list_main", "MOBA_PO") == True
    assert dal.get_imported_file_path("closing_list_main", "MOBA_PO") is None
    moba_po_files_after_remove = dal.get_imported_files_by_entity("MOBA_PO")
    assert len(moba_po_files_after_remove) == 1
    assert dal.remove_imported_file("non_existent_key", "MOBA_PO") == False
    logging.info("remove_imported_file tested.")

    # 測試 clear_imported_files_by_entity
    assert dal.clear_imported_files_by_entity("MOBA_PO") == True
    moba_po_files_after_clear = dal.get_imported_files_by_entity("MOBA_PO")
    assert len(moba_po_files_after_clear) == 0
    spx_files_not_cleared = dal.get_imported_files_by_entity("SPX") # 確保其他實體的沒被清除
    assert len(spx_files_not_cleared) == 1
    logging.info("clear_imported_files_by_entity tested.")

    logging.info("--- DAL operations testing completed. ---")

    if os.path.exists(db_file):
        os.remove(db_file)
        logging.info(f"Cleaned up test database: {db_file}")
```
