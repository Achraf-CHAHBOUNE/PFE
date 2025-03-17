from extractor import Extractor
from loader import Loader
from config import SOURCE_CONFIG, DESTINATION_CONFIG

class Orchestrator:
    def __init__(self):
        self.extractor = Extractor(SOURCE_CONFIG)
        self.loader = Loader(DESTINATION_CONFIG)

    def process_orchestration(self):
        """Orchestrate the extraction and loading process."""
        try:
            self.extractor.process_tables_names()
            tables = self.extractor.extract_tables_names()
            for table in tables:
                offset = 0
                while True:
                    data = self.extractor.extract_table_data(table, offset)
                    if not data:
                        break
                    self.loader.load_batch_into_database(table, data)
                    offset += len(data)
        except Exception as e:
            print(f"❌ Error during orchestration: {e}")
            raise