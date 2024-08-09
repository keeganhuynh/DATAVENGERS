import traceback
import os

from core.vector_store import VectorStore
from core.embedding import Embedding
from common.config_loader import ConfigLoader
from common.logger import Logger

class InsertCSV(Logger):
    def __init__(self, vector_store_name):
        super().__init__()
        self.VectorStore = VectorStore()
        self.embedding = Embedding().get_embedding()
        config_loader = ConfigLoader()
        self.default_vector_stores_saving_path = config_loader.config_data['DEFAULT_VECTOR_STORE_SAVING_PATH']
        self.vector_store_name = vector_store_name
        try:
            vector_store_path = f"{self.default_vector_stores_saving_path}/{vector_store_name}.faiss"
            if not os.path.exists(vector_store_path):                
                self.info(
                    message=f'No local {vector_store_name} vector store was found'
                )
                self.vector_store = self.VectorStore.creat_vector_store(self.embedding)
                self.vector_store.save_local(
                    folder_path=self.default_vector_stores_saving_path, 
                    index_name=self.vector_store_name
                )
            else:
                self.vector_store = self.VectorStore.load_local_vector_store(vector_store_name, self.embedding)
        except:
            self.error(
                message= f"Can not load your vector store, traceback = {traceback.format_exc()}"

            )
         
    def insert_csv(self, csv_path):
        self.VectorStore.insert_document(self.vector_store, csv_path)



            

    
