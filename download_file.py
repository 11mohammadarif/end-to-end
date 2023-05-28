import requests

# List of URLs for each month
urls = [
        "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-01.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-02.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-03.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-04.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-05.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-06.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-07.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-08.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-09.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-10.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-11.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-12.parquet"
    ]

# Directory path to save the files
save_dir = '/home/mohammadarif/cap'

for url in urls:
    filename = url.split("/")[-1]
    save_path = f"{save_dir}/{filename}" 
    response = requests.get(url)

    with open(save_path, "wb") as file:
        file.write(response.content)