from handler.insert_csv import InsertCSV

def main(vector_store_name, csv_path):
    app = InsertCSV(vector_store_name)
    app.insert_csv(csv_path)

if __name__ == '__main__':
    main("test1", "food_data.csv")