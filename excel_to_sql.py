import pandas as pd
import sqlite3
import os

class DataMigrationEngine:
    """Professional ETL tool to migrate data from Excel to structured SQL."""
    
    def __init__(self, db_name="business_data.db"):
        self.db_name = db_name
        print(f"🗄️ Database Engine Ready: {self.db_name}")

    def migrate(self, excel_file):
        """Processes the Excel file and moves data to a SQLite database."""
        if not os.path.exists(excel_file):
            print(f"❌ Error: Source file '{excel_file}' not found!")
            return

        try:
            print(f"🔄 Reading data from {excel_file}...")
            # خواندن فایل اکسل
            df = pd.read_excel(excel_file)
            
            # پاکسازی نام ستون‌ها (برای استاندارد SQL)
            df.columns = [c.replace(' ', '_').lower() for c in df.columns]
            
            # اتصال به دیتابیس SQL (اگر نباشد ساخته می‌شود)
            conn = sqlite3.connect(self.db_name)
            
            # انتقال داده‌ها به جدول 'products_table'
            df.to_sql("migrated_products", conn, if_exists='replace', index=False)
            
            print(f"✅ Success! Data migrated to SQL Database.")
            
            # اجرای یک کوئری تستی برای نمایش خروجی
            print("\n📊 Verification: Fetching first 3 rows from SQL:")
            query_result = pd.read_sql("SELECT * FROM migrated_products LIMIT 3", conn)
            print(query_result)
            
            conn.close()
            print("\n🏁 Database connection closed.")
            
        except Exception as e:
            print(f"🚨 Migration Failed: {e}")

if __name__ == "__main__":
    # ایجاد نمونه از موتور مهاجرت
    engine = DataMigrationEngine()
    
    # اجرای عملیات روی فایل اکسل
    # مطمئن شو نام فایل دقیقاً همانی باشد که در پوشه کپی کردی
    engine.migrate("API_Products_Report.xlsx")