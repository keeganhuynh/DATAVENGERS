import pandas as pd

from common.logger import Logger
from langchain_core.documents import Document


class Preprocessor(Logger):
    def __init__(self):
        super().__init__()
        pass

    def format_csv(self, csv_file_path: str) -> Document:
        """
        Convert your content to right format

        Args:
        - Content (dict): your content save in dict type
        """
        df = pd.read_csv(csv_file_path)

        documents = []
        for _, row in df.iterrows():
            page_content = row['content']
            metadata = {
                'field': row.get('field', 'unknown')
            }
            document = Document(page_content=page_content, metadata=metadata)
            documents.append(document)
        
        return documents

# test = Preprocessor()
# print(test.format_csv('/home/hhkhanh/DATAVENGERS/src/core/ex.csv'))
