import asyncio
from core.crawler import DataCrawler

def main():
    # Define the base URL to start crawling from
    base_url = "https://tuyensinh.uel.edu.vn"

    # Define the maximum depth for recursive crawling
    max_depth = 3

    # Define paths for saving the results
    extracted_files_dir = "src/database/extracted_files"
    download_folder = f"{extracted_files_dir}/pdf_files"
    output_csv = f"{extracted_files_dir}/page_contents_v3.csv"

    # Initialize the DataCrawler
    crawler = DataCrawler(
        base_url=base_url,
        max_depth=max_depth,
        download_folder=download_folder
    )

    # Start the crawling and extraction process asynchronously
    asyncio.run(crawler.start_extract(output_csv=output_csv))

    print(f"Crawling and extraction completed. Results saved to {output_csv} and PDFs saved in {download_folder}.")

if __name__ == "__main__":
    main()
