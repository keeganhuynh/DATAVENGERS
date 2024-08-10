import traceback

from common.logger import Logger

class  Retriever(Logger):
    def __init__(self):
        super().__init__()
        pass
    
    def query_vector(self, vector_store, context, filter, k=5):
        try:
            return vector_store.similarity_search(
                f"{context}",
                k=k,
                filter=filter,
            )
        except:
            self.error(
                message= f"Can not query your context, traceback = {traceback.format_exc()}"
            )