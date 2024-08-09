import faiss
import traceback

from common.config_loader import ConfigLoader
from common.logger import Logger
from core.preprocesor import Preprocessor

from uuid import uuid4
from langchain_community.docstore.in_memory import InMemoryDocstore
from langchain_community.vectorstores import FAISS



class VectorStore(Logger):
    def __init__(self) -> None:
        super().__init__()
        pass

    def creat_vector_store(self, embeddings):
        '''
        Create a new vector store 

        Args:
        - embeddings (object): embedding model

        Output:
        - FAISS vector store
        '''
                                                  
        embedding_dimesion = len(embeddings.embed_query("I am Vietnamese"))

        index = faiss.IndexFlatL2(embedding_dimesion)

        vector_store = FAISS(
            embedding_function=embeddings,
            index=index,
            docstore=InMemoryDocstore(),
            index_to_docstore_id={},
        )

        self.info(
                message=f'Create a new vector store, save to local'
        )
        return vector_store

    def load_local_vector_store(self, vector_store_name, embeddings) -> object:
        """
        Load an exist local vector store

        Args:
        - vector_store_name (str): your local vector store name
        - embeddings (list): embedding model
        """
        try:
            vector_store = FAISS.load_local(
                f"{vector_store_name}", embeddings, allow_dangerous_deserialization=True
            )
            self.info(
                message=f'Load vector store {vector_store_name} success'
            )
            return vector_store
        except:
            self.error(
                message= f"Can not load your vector store, traceback = {traceback.format_exc()}"
            )

    def insert_document(self, vector_store, csv_file):
        """
        Insert list of document to the vector store

        Args:
        - vector_store (object): your local vector store name
        - csv_file: your csv path
        """
        try:
            preprocessor = Preprocessor()
            documents = preprocessor.format_csv(csv_file)
            uuids = [str(uuid4()) for _ in range(len(documents))]
            vector_store.add_documents(documents=documents, ids=uuids)
            self.info(
                message=f"Insert {len(documents)} to your vector store"
            )
        except:
            self.error(
                message= f"Can not insert your contents to vector store, traceback = {traceback.format_exc()}"
            )
