import sys
import logging
import time
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
from core import pubmed_search
from core import pubmed_batch_download
import uuid
import multiprocessing as mp
from snowflake.connector import connect as Connect

# Set up logging
logging.basicConfig(filename='pubmed.log', level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

conn_params = {
    "user": "API_DATA_USER",
    "password": "APIDATAUser@2024",
    "account": "dn13102.us-east-1",
    "warehouse": "DATA_LOAD_WH",
    "database": "INSIGHTSDWDEV",
    "schema": "PUBMED"
}

# pubmed_log_id = None


class CustomError(Exception):
    def __init__(self, message):
        self.message = message


def get_cursor():
    connection = Connect(**conn_params)
    cursor = connection.cursor()

    return connection, cursor


def update_log_table(pubmed_log_id, processed=None, error_log=None, status="FINISHED"):
    try:
        # global pubmed_log_id
        connection, cursor = get_cursor()
        if error_log:
            cursor.execute("""
                UPDATE PUBMED_DATA_LOG 
                SET ERROR_LOG = %s
                WHERE PUBMED_LOG_ID = %s
            """, (error_log, pubmed_log_id))

            query = f"""
                UPDATE PUBMED_DATA_LOG 
                SET ERROR_LOG = {error_log}
                WHERE PUBMED_LOG_ID = {pubmed_log_id}"""

            logging.info(f"Query with processed: {query}")

        else:
            cursor.execute("""
                UPDATE PUBMED_DATA_LOG 
                SET PROCESSED = %s, END_TIME = CURRENT_TIMESTAMP(),STATUS = %s
                WHERE PUBMED_LOG_ID = %s
            """, (processed, status, pubmed_log_id))
            query = f"""
                UPDATE PUBMED_DATA_LOG 
                SET PROCESSED = {processed}, END_TIME = CURRENT_TIMESTAMP(),STATUS = {status}
                WHERE PUBMED_LOG_ID = {pubmed_log_id}"""

            logging.info(f"Query with processed: {query}")
        cursor.close()
        connection.commit()
    except Exception as e:
        logging.error("Error updating Log Table into Snowflake: %s, for logid %s", e, pubmed_log_id)
        exit(0)


# Function to insert a row into the table
def insert_to_log_table(row):
    try:
        connection, cursor = get_cursor()
        columns = ', '.join(row.keys())
        values = ', '.join('%s' for _ in row)
        sql = f"""
            INSERT INTO INSIGHTSDWDEV.PUBMED.PUBMED_DATA_LOG 
                ({columns},START_TIME)
            VALUES 
                ({values},CURRENT_TIMESTAMP())
        """
        # print(sql)
        # print(tuple(row.values()))
        # Execute the SQL query with values
        cursor.execute(sql, tuple(row.values()))
        cursor.close()
        connection.commit()
    except Exception as e:
        logging.error("Error while inserting log Table into Snowflake: %s, Search key word %s", e,
                      row["SEARCH_KEYWORD"])
        exit(0)


# Function to insert data into the table
def insert_to_datatbl(data_rows, start, end, pubmed_log_id):
    try:
        connection, cursor = get_cursor()
        # Extracting data from data_rows and preparing it for bulk insert
        # logging.info(f"Insert data {start} {end} \n\n {data_rows} \n\n")

        values = [(row['PMID'], row['SEARCH TERM'], row['TITLE'], str(row['ABSTRACT']),
                   row['AUTHOR'], row['KEYWORDS'], row['PMC'], row['PUBDATE'], pubmed_log_id) for row in data_rows]

        # Performing bulk insert
        cursor.executemany("""
            INSERT INTO PUBMED_DATA (PMID, SEARCH_TERM, TITLE, ABSTRACT, AUTHOR_LIST, KEYWORD_LIST, PMCID, PUBDATE, PUBMED_LOG_ID)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""", values)


        logging.info(f"Query executed for {start} {end}")
        cursor.close()
        connection.commit()
    except Exception as e:
        logging.error(f"Exception while insert into Data table {e} for Records {start} to {end}")
        raise e


def process_batch(args):
    start, end, search_term, search_results, batch_size, pubmed_log_id = args
    logging.info("Going to download record %i to %i" % (start, end))
    try:
        result = pubmed_batch_download(search_term, search_results, batch_size, start)
        if result:
            insert_to_datatbl(result, start, end, pubmed_log_id)
            logging.info(f"Downloaded Data {start} to {end},retrieved {len(result)}")
            return len(result)
        logging.error(f"Unable to download {start} to {end}")
        raise CustomError ("No records returned from core")
    except Exception as e:
        logging.error(f"Error inserting data into Snowflake: {e} for Records {start} to {end}")
        update_log_table(pubmed_log_id, error_log=str(e), status="Finished with Exceptions")
        logging.info("Updated log Table with exceptions")
        return 0
        # raise e


def fetch_and_upload(search_results, search_term, count, min_date, max_date, pubmed_log_id):

    processed = 0
    try:
        batch_size = 1000
        num_processes = 6  # Number of CPU cores
        logging.info(f"Started Multi Processing {time.asctime()}")
        logging.info(f"processing {count} records with {num_processes} processors")

        processed_counts = []
        # results = []

        # Create a thread pool with the desired number of threads
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_processes) as exe:
            results = []

            for start in range(0, count, batch_size):
                end = min(count, start + batch_size)
                # if processed and processed % 9000 == 0:
                #     logging.info(f"Refreshing results")
                #     time.sleep(2)
                #     count, search_results = pubmed_search(search_term, min_date=min_date, max_date=max_date)
                results.append(exe.submit(process_batch, (start, end, search_term, search_results, batch_size, pubmed_log_id)))

                if len(results) == num_processes//2:  # Check if 10 tasks have been submitted
                    # Retrieve the results from the submitted tasks and sum them up
                    for result in results:
                        processed_counts.append(result.result())
                    processed += sum(processed_counts)
                    logging.info(f"Processed {len(processed_counts)} tasks. Total processed: {processed}")
                    processed_counts = []  # Reset the processed counts
                    results = []  # Reset the results list

            # Handle the remaining tasks
            for result in results:
                processed_counts.append(result.result())
            processed += sum(processed_counts)
            logging.info(f"Finally Processed records: {processed}")


        # Create a pool of worker processes
        # with ThreadPoolExecutor(num_processes) as exe:
        #     # execute tasks concurrently and process results in order
        #     for start in range(0, count, batch_size):
        #         end = min(count, start + batch_size)
        #         results.append(exe.submit(process_batch, (start, end, search_term, search_results, batch_size, pubmed_log_id)))
        #             # print(result)
        #
        #         try:
        #             # Retrieve the results from the submitted tasks and sum them up
        #             for result in results:
        #                 processed_counts.append(result.result())
        #             processed += sum(processed_counts)
        #             # logging.info(f"number of results: {len(results)}")
        #             processed_counts = []
        #             results = []
        #
        #         except Exception as e:
        #             logging.info(f"Processed: {processed}")

        # with mp.Pool(processes=num_processes) as pool:
        #     for start in range(0, count, batch_size):
        #         end = min(count, start + batch_size)
        #         processed_counts.append(pool.apply_async(process_batch, args=((start, end, search_term, search_results, batch_size, pubmed_log_id),)))
        #     try:
        #         processed_counts = [result.get() for result in processed_counts]
        #     except Exception as e:
        #         logging.info(f"Processed Counts {processed_counts}")
        #         processed = sum(processed_counts)
        #         logging.info(f"Ended multi processing {time.asctime()}")
        #         # raise e
        logging.info(f"Ended multi processing {time.asctime()}")
        update_log_table(pubmed_log_id, processed=processed)
    except Exception as e:
        logging.error("An error occurred in the fetch_and_upload: %s", e)
        update_log_table(pubmed_log_id, processed=processed, error_log=str(e), status="Finished with Exceptions")


if __name__ == "__main__":
    try:
        # global pubmed_log_id
        pubmed_log_id = str(uuid.uuid4())
        logging.info(f"Ingestion Application Started")
        logging.info(f"pubmed_log_id: {pubmed_log_id}")
        if len(sys.argv) <= 2:
            logging.error("Usage: python ingest.py <search_term>")
            sys.exit(1)
        search_term = sys.argv[1]
        logging.info(f"Received request: {search_term}")
        min_date, max_date = sys.argv[2], sys.argv[3]
        logging.info(f"Received request With Parameters: {search_term},{min_date},{max_date}")
        if min_date == "None" or max_date == "None":
            min_date = None
            max_date = None
        count, search_results = pubmed_search(search_term, min_date=min_date, max_date=max_date)
        log_table_entry = {"PUBMED_LOG_ID": pubmed_log_id,
                           "SEARCH_KEYWORD": search_term,
                           "TOTAL_PMIDS": count,
                           "STATUS": "STARTED",
                           "FROM_DATE": min_date,
                           "TO_DATE": max_date}
        insert_to_log_table(log_table_entry)
        # exit(0)
        fetch_and_upload(search_results, search_term, count, min_date, max_date, pubmed_log_id)
        logging.info(f"Ingestion Application Stopped Successfully")
    except Exception as e:
        logging.error("An error occurred in the main process: %s", e)
        logging.info(f"Ingestion Application Stopped With Exception")