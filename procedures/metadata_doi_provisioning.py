#
# EarthCODE metadata DOI provvisioning
#
# Project: EarthCODE
#
# Center for Sensing Solutions (Eurac research)
#
# file: metadata_doi_provisioning.py
#

import sys
import os
import time
import datetime
import logging
import requests

from process_json import *

data_cite_url_prod = "https://api.datacite.org"
data_cite_url_test = "https://api.test.datacite.org"

data_cite_user = os.getenv('DATACITE_USER')
data_cide_p =  os.getenv('DATACITE_P')

doi_prefix = "10.80823"
catalog_base_url = "https://opensciencedata.esa.int/products/"

# todo: set the url to 'test' or 'production' environment
data_cite_url = data_cite_url_test

log_active = False
log_file = "metadata_doi_provisioning"

# if 'True' writes the doi decorated json metadata to a new file -> used for testing
writeMtdToNewFile = False

# prints to stdout and to log file if active (log_active = True)
def printLog(msg):
  print(msg)
  if(log_active):
    logger.info(msg)


# checks if the doi is contained in the map and is not empty
def doiExists(mtd_map):
  return "doi" in mtd_map and mtd_map["doi"] != ""

# extracts the prefix from the doc
def extractPrefix(doi_val):
  doi_parts = doi_val.split("/")
  if(len(doi_parts) > 1):
    return doi_parts[0]
  else:
    return "null"

# retrieve the doi metadata from datacite
def retrieveDoiMtd(doi_val):
  try:
    res = requests.get(data_cite_url + "/dois/" + doi_val)

    printLog(f"status_code: {res.status_code}")

    if(res.status_code == 200):
      printLog("successful")
      #printLog("doi:")
      #printLog(res.text)
      doi_map = json.loads(res.text)
      return doi_map
  except:
    printLog(f"Error in retrieving doi: {res.status_code}")


def isDoiMtdUpToDate(mtd_map, doi_map):
  printLog("checking if datacite metadata is up to date")
  printLog("mtd_map:")
  # to be implemented
  #printLog(mtd_map)
  #printLog("doi_map:")
  #printLog(doi_map)
  return (mtd_map["publ_year"] == doi_map["publicationYear"])

# post the doi publish request
def postDoiReq(mtd_map):
  title=mtd_map["title"]
  publ_year=mtd_map["publ_year"]

  # todo: update prefix
  req_obj = """
  {
    "data": {
        "type": "dois",
        "attributes": {
        "event": "publish",
        "prefix": "doi_prefix_p",
        "creators": [
          {
            "name": "producer_p"
          }
        ],
        "titles": [
          {
            "lang": "en",
            "title": "title_p"
          }
        ],
        "publisher": "publisher_p",
        "publicationYear": publ_year_p,
        "types": {
          "resourceTypeGeneral": "resource_type_p"
        },
        "url": "url_p"
      }
    }
  }
  """

  # note: multiline strings appearently have problems with json content when formatting variables, so we use a siple replace

  req_obj = req_obj.replace("doi_prefix_p", doi_prefix)
  req_obj = req_obj.replace("producer_p", mtd_map["producer"])
  req_obj = req_obj.replace("title_p", title)
  req_obj = req_obj.replace("publ_year_p", f"{publ_year}")
  req_obj = req_obj.replace("resource_type_p", mtd_map["resource_type"])
  req_obj = req_obj.replace("publisher_p", mtd_map["publisher"])
  req_obj = req_obj.replace("url_p", mtd_map["url"])

  #print("req_obj-> " + req_obj)

  printLog("posting doi request...")

  try:
    url = data_cite_url + "/dois"
    res = requests.post(url, data = req_obj, auth = (data_cite_user, data_cide_p), headers={"Content-Type":"application/vnd.api+json"})

    printLog(f"status: {res.status_code}")

  except:
    printLog("Error in posting request")
    exit -1

  if(res.status_code != 201):
    printLog("response: " + res.text)

  return res

# update do 
def postDoiMtdUpdate(doi_id, mtd_map):
  printLog("updating doi mtd fields...")
  req_obj="""
  {
    "data": {
      "type": "dois",
      "attributes": {
        "publicationYear": publ_year_p
      }
    }
  }
  """
  publ_year = mtd_map["publ_year"]
  req_obj = req_obj.replace("publ_year_p", publ_year)

  printLog("posting doi update...")
  try:
    url = data_cite_url + "/dois/" + doi_id
    printLog("URL: " + url)
    res = requests.post(url, data = req_obj, auth = (data_cite_user, data_cide_p), headers={"Content-Type":"application/vnd.api+json"})

    printLog(f"status: {res.status_code}")
  except:
    printLog("Error in update request")
    exit -1

  if(res.status_code != 200):
    printLog("response: " + res.text)


# extract doi
def extractDoi(res_obj):
	json_obj = json.loads(res_obj)
	return json_obj['data']['attributes']['doi']

# insert doi in STAC metadata json
def insertDoi(json_file_name, doi_val):
  printLog("adding doi element: " + doi_val + " to file: " + json_file_name)
  try:
    with open(json_file_name, 'r', encoding='utf-8') as file:
      data = json.load(file)
      data["sci:doi"] = doi_val
      
      modified_json = json.dumps(data, indent=2)

    #printLog("modified_json:")
    #printLog(modified_json)

    if(writeMtdToNewFile):
      json_file_name = json_file_name + "TEST"

    with open(json_file_name, "w") as f_out:
      f_out.write(modified_json)
      printLog("new file written to: " + json_file_name)

      return True
  except:
    printLog("Error in writing metadata file")
    return False

if __name__ == '__main__':
  if(log_active == True):
    print("init log file...")
    logger = logging.getLogger(log_file)
    hdlr = logging.FileHandler('./logs/' + log_file + ".log")
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr) 
    logger.setLevel(logging.INFO)

  printLog("starting metadata DOI provisioning...")

  if(len(sys.argv) > 1):
    file_to_reg = sys.argv[1]

    printLog("processing file: " + file_to_reg)

    printLog("parsing metadata...")

    mtd_fields_json = process_json(file_to_reg)

    if not mtd_fields_json:
      printLog("unable to parse file")
      exit -1

    publ_year = datetime.now().year

    url_attr = catalog_base_url + mtd_fields_json.id + "/collection"

    print("url_attr: " + url_attr)

    mtd_fields =  {
      "title": mtd_fields_json.title,
      "description": mtd_fields_json.description,
      "publ_year": publ_year,
      "resource_type": "Dataset",
      "producer": mtd_fields_json.providers[0].name,
      "publisher": mtd_fields_json.providers[2].name,
      "url": url_attr
    }

    doi_attr = getattr(mtd_fields_json, "sci:doi", None)

    if(doi_attr != None and doi_attr != ""):
        mtd_fields["doi"] = doi_attr

    #printLog("mtd_fields: ")
    #printLog(mtd_fields)

    doi_e = doiExists(mtd_fields)
    printLog("doi exists: " + str(doi_e))

    if(not doi_e):

      printLog("performing doi request...")

      resp = postDoiReq(mtd_fields)

      doi_elem = "null"

      if(resp.status_code == 201):
        printLog("response successful, extracting doi...")

        #printLog("resp: " + resp.text)

        doi_elem = extractDoi(resp.text)

        printLog("doi_elem: " + doi_elem)

      if(doi_elem != "null"):

        printLog("metadata doi created: " + doi_elem)

        if(insertDoi(file_to_reg, doi_elem)):
          printLog("doi inserted successfully in metadata")
        else:
          printLog("Error in inserting doi")
    else:
      printLog("doi already exists: " + mtd_fields["doi"])

      doi_prefix_mtd = extractPrefix(mtd_fields["doi"])

      printLog("doi_prefix_mtd: " + doi_prefix_mtd)

      printLog("checking if datacite metadata is up to date")

      doi_map = retrieveDoiMtd(mtd_fields["doi"])

      updated = isDoiMtdUpToDate(mtd_fields, doi_map)

      if(not updated):
        printLog("checking if prefix matches...")

        if(doi_prefix_mtd == doi_prefix):
          printLog("performing doi update...")

          resp = postDoiMtdUpdate(mtd_fields["doi"], mtd_fields)

          if(resp.status_code == 201):
            printLog("update successful")
          else:
            printLog("Error in updating doi metadata")
        else:
          printLog("doi prefix mismatch, skipping...")
      else:
        printLog("doi metadata is up to date")
  else:
      printLog("no file provided")
