{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "274766f7",
   "metadata": {},
   "outputs": [],
   "source": [
    "**Athena Function**\n",
    "\n",
    "Dict = {}\n",
    "def download_and_load_query_results(\n",
    "    client: boto3.client, query_response: Dict\n",
    ") -> pd.DataFrame:\n",
    "    while True:\n",
    "        try:\n",
    "            client.get_query_results( QueryExecutionId=query_response[\"QueryExecutionId\"] )\n",
    "            print( client.get_query_results( QueryExecutionId=query_response[\"QueryExecutionId\"] ))\n",
    "            break\n",
    "        except Exception as err:\n",
    "            if \"not yet finished\" in str(err):\n",
    "                time.sleep(0.001)\n",
    "            else:\n",
    "                raise err\n",
    "    \n",
    "    temp_file_location: str = \"anthena_query_results.csv\"\n",
    "    s3_client = boto3.client(\n",
    "        \"s3\",\n",
    "         aws_access_key_id=AWS_ACCESS_KEY,\n",
    "    aws_secret_access_key=AWS_SECRET_KEY,\n",
    "    region_name=AWS_REGION\n",
    "        )\n",
    "        \n",
    "    s3_client.download_file(\n",
    "            S3_BUCKET_NAME, \n",
    "            f\"{S3_OUTPUT_DIRECTORY}/{query_response['QueryExecutionId']}.csv\",\n",
    "            temp_file_location\n",
    "            \n",
    "          )\n",
    "    return pd.read_csv(temp_file_location)\n",
    "\n",
    "response = athena_client.start_query_execution(\n",
    "    QueryString=\"SELECT * FROM engima_jhud\",\n",
    "    QueryExecutionContext={\"Database\":SCHEMA_NAME},\n",
    "    ResultConfiguration={\n",
    "        \"OutputLocation\":S3_STAGING_DIR \n",
    "    }\n",
    ")"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.9.13"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
