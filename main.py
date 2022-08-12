# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

import os

import botocore
from dotenv import load_dotenv
import boto3
import logging

import pyarrow.parquet as pq
import s3fs
s3 = s3fs.S3FileSystem()


import pandas as pd
import geopandas as gp


import random
from shapely.geometry import Point

from sortedcontainers import SortedList

load_dotenv()

s3_client = boto3.client(os.getenv('AWS_S3'), region_name=os.getenv('AWS_REGION'))
bucket_name = os.getenv("S3_BUCKET")
sortedList = SortedList()

s3_resource = boto3.resource(os.getenv('AWS_S3'))


def upload_dataset_to_s3(name,s3_key):
    """ Upload Dataset """
    response = s3_client.upload_file(name, bucket_name, s3_key)
    logging.info('Uploaded the picture to S3 {}'.format(response))
    s3Url = "{0}.s3.amazonaws.com/{1}".format(bucket_name, s3_key)
    s3Url = "https://" + s3Url.replace(" ", "+").replace(":", "%3A")
    print('Uploaded the picture to S3 {}'.format(s3Url))

    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
def load_datasets():
    # global tiles
    print("start load")
    tiles = pq.ParquetDataset('s3://x20208057-research-project/complete-dataset.parquet', filesystem=s3).read_pandas().to_pandas()
    print(tiles.head(5))
    tiles['tile'] = gp.GeoSeries.from_wkt(tiles['tile'])
    global geoDataFrame
    geoDataFrame = gp.GeoDataFrame(tiles, geometry='tile')
    geoDataFrame.set_crs('epsg:3857')
    print(geoDataFrame.head(5))

    try:
        s3_resource.Bucket(bucket_name).download_file('world-boundary.zip', 'local_world_boundary_dataset.zip')
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise
    worldCountries = gp.read_file("zip://local_world_boundary_dataset.zip")
    global geoWorldCountries
    geoWorldCountries = gp.GeoDataFrame(worldCountries, geometry='geometry')
    print(geoWorldCountries.head(5))


def group_by_countries():
    joinGeoWorldCountries = geoDataFrame.sjoin(geoWorldCountries, how="left", predicate='intersects')
    joinGeoWorldCountries['avg_d_mbps'] = joinGeoWorldCountries['avg_d_kbps'] / 1000
    joinGeoWorldCountries['avg_u_mbps'] = joinGeoWorldCountries['avg_u_kbps'] / 1000
    joinGeoWorldCountries.to_csv('joinGeoWorldCountries.csv')
    upload_dataset_to_s3('joinGeoWorldCountries.csv','2020-quarter3-dataset-joinWorld.csv')


def simulation_dataset_by_country(countryName):
    # print("start simulation")
    # response = s3_client.get_object(Bucket='x20208057-research-project', Key="2020-quarter3-dataset-joinWorld.csv")

    # status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    # if status == 200:
    #     print(f"Successful S3 get_object response. Status - {status}")
    global shapeTest
    shapeTest = pd.read_csv('joinGeoWorldCountries.csv')
    for col in shapeTest.columns:
        print(col)
    shapeTest = shapeTest.drop(shapeTest.columns[[1, 3, 4, 8, 9, 10, 11, 12, 13, 15, 16]], axis=1)
    shapeTest['tile'] = gp.GeoSeries.from_wkt(shapeTest['tile'])
    shapeTestFormated = gp.GeoDataFrame(shapeTest, geometry='tile')
    countryBoundaryJoined = shapeTestFormated[shapeTestFormated['name'].isin([countryName])]
    countryBoundaryJoined = countryBoundaryJoined.reset_index()
    # for col in shapeTestFormated.columns:
    #     print(col)
    print(countryBoundaryJoined['tile'].head(1))
    countryTilesBounds = countryBoundaryJoined['tile'].bounds
    min_x = countryTilesBounds['minx'].min()
    min_y = countryTilesBounds['miny'].min()
    max_x = countryTilesBounds['maxx'].max()
    max_y = countryTilesBounds['maxy'].max()
    # print(min_x)
    # print(min_y)
    # print(max_x)
    # print(max_y)
#     global min_x
#     min_x = 68.3843994140625
#     global min_y
#     min_y = 8.07554603328031
#     global max_x
#     max_x = 96.4984130859375
#     global max_y
#     max_y = 32.9164853473144

    worldCountries = gp.read_file("zip://local_world_boundary_dataset.zip")
    geoWorldCountries = gp.GeoDataFrame(worldCountries, geometry='geometry')
    global countryDefinedBoundary
    countryDefinedBoundary = geoWorldCountries.loc[geoWorldCountries["name"].isin([countryName])]
    print(countryDefinedBoundary.head(1))

    output = pd.DataFrame()
    for j in range(10):
        output = pd.DataFrame()
        for i in range(1000):
            points = polygon_random_points(countryDefinedBoundary, 1)
            point = points.geometry
            output = output.append({'Point': points.geometry}, ignore_index=True)
            polygon = countryBoundaryJoined.geometry
            out = point.apply(lambda p: polygon.distance(p))
            countryBoundaryJoined['distance'] = out.T
            # countryBoundaryJoined = countryBoundaryJoined.sort_values('distance')
            sortOutput = countryBoundaryJoined[countryBoundaryJoined['distance'] <= 1]
            output = output.append(sortOutput.sort_values('distance'))
            print(i)

        print(j)
        resultOutput = 'output{0}.csv'.format(j)
        output.to_csv(resultOutput)
        # upload_dataset_to_s3(resultOutput, 'indiaOutputs/{0}'.format(resultOutput))
        # if os.path.exists(resultOutput):
        #     os.remove(resultOutput)
        # else:
        #     print("The file does not exist")

def polygon_random_points (poly, num_points):
    points = gp.GeoDataFrame()
    while len(points) < num_points:
        random_point = Point([random.uniform(min_x, max_x), random.uniform(min_y, max_y)])
        geometry = [Point(random_point.x,random_point.y)]
        geo_df = gp.GeoDataFrame(geometry = geometry,crs=4326)
        points_within = geo_df.sjoin(countryDefinedBoundary, how="left", predicate='intersects')
        print(points_within)
        status = points_within['name'].isna().bool()
#         print(status)
        if (status==False):
            points=geo_df
            break;
    return points


def download_dataset_s3(s3File,localFile):
    try:
        s3_resource.Bucket(bucket_name).download_file(s3File,localFile)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise


if __name__ == '__main__':
    # upload_dataset_to_s3('/Users/sathish/NCI/Final Project/Works/combinedDataset.parquet','complete-dataset.parquet')
    # upload_dataset_to_s3('/Users/sathish/Code/Research Project/Simulation/world-administrative-boundaries.zip', 'world-boundary.zip')
    # load_datasets()
    # group_by_countries()
    download_dataset_s3('world-boundary.zip', 'local_world_boundary_dataset.zip')
    download_dataset_s3('2020-quarter3-dataset-joinWorld.csv', 'joinGeoWorldCountries.csv')
    simulation_dataset_by_country('India')
    # upload_dataset_to_s3('output0.csv', 'india-prescript/output2.csv')
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
