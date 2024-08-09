import os
import traceback

from common.config_loader import ConfigLoader
from common.logger import Logger

from langchain_google_genai import GoogleGenerativeAIEmbeddings

class Embedding(Logger):
    def __init__(self) -> None:
        super().__init__()

        config_loader = ConfigLoader()
        gg_api_key = config_loader.config_data['GOOGLE_API_KEY']
        os.environ["GOOGLE_API_KEY"] = gg_api_key
        model_id = config_loader.config_data['MODEL_ID']
        try:
            self.embedding = GoogleGenerativeAIEmbeddings(model=model_id)
        except:
            self.error(
                message= f"Can not load embedding from google vertex AI, traceback = {traceback.format_exc()}"
            )
            

    def get_embedding(self) -> object:
        '''
        Take embedding from google vertex AI
        '''                    
        return self.embedding