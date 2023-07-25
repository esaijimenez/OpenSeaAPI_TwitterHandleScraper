#Imports
import pandas as pd
import requests as r
import os
import time


#Dataframe Settings
pd.set_option('display.max_colwidth', 50)

#Methods
def testWallets(raw_csv: pd.DataFrame, csv_to_use) -> bool:
    oneHeaderInCSV = len(raw_csv.columns)
    if oneHeaderInCSV == 1:
        api_key = ""
        directory_path = ""
        clean_directory_path = ""
        
        wallet_addresses = raw_csv
        
        wallet_address_list = []
        
        for address in wallet_addresses.index:
            wallet_address_list.append(wallet_addresses['Wallet Address'][address])
        
        url_address_list = []
        
        for address in wallet_address_list:
            url_list = "https://api.opensea.io/api/v1/account/" + address
            url_address_list.append(url_list)
            
        twitter_list = []
        batch = 0
        for url in url_address_list:
            headers = {
                "accept": "application/json",
                "X-API-KEY": api_key
                }
            response = r.get(url, headers=headers)
            data = response.json()
            
            try:
                sd = data["data"]["twitter_username"]
                twitter_list.append(sd)
            except:
                data["c"] = "None"
                sd = data["c"]
                twitter_list.append(sd)
            batch += 1
            if batch == 6:
                batch = 0    
                time.sleep(3)

        twitter = pd.DataFrame(twitter_list) 
            
        clean_csv = pd.concat([wallet_addresses, twitter], axis=1)  

        print(clean_csv)
        
        clean_csv.to_csv(f'{clean_directory_path}clean{csv_to_use}', index=False)  
        archive_directory_path = ""
        
        try:
            os.rename(f'{directory_path}{csv_to_use}',f'{archive_directory_path}{csv_to_use}')
            
        except:
            os.remove(f'{archive_directory_path}{csv_to_use}')
            os.rename(f'{directory_path}{csv_to_use}',f'{archive_directory_path}{csv_to_use}')
        
        return True
    return False

def openSea(raw_csv: pd.DataFrame) -> pd.DataFrame:
    api_key = ""
    wallet_addresses = raw_csv
    
    wallet_address_list = []
    
    for address in wallet_addresses.index:
        wallet_address_list.append(wallet_addresses['To'][address])
    
    url_address_list = []
    
    for address in wallet_address_list:
        url_list = "https://api.opensea.io/api/v1/account/" + address
        url_address_list.append(url_list)
        
    twitter_list = []
    batch = 0
    for url in url_address_list:
        headers = {
            "accept": "application/json",
            "X-API-KEY": api_key
            }
        response = r.get(url, headers=headers)
        data = response.json()
        
        try:
            sd = data["data"]["twitter_username"]
            twitter_list.append(sd)
        except:
            data["c"] = "None"
            sd = data["c"]
            twitter_list.append(sd)
        batch += 1
        if batch == 6:
            batch = 0    
            time.sleep(3)

    twitter = pd.DataFrame(twitter_list) 
        
    horizontal_concat = pd.concat([wallet_addresses, twitter], axis=1)  
    return horizontal_concat
    

def OpenSea_TwitterHandleScraping():
    directory_path = ""
    directory = os.listdir(directory_path)
    
    csv_to_use = None
    
    for csv in directory:
        if csv.endswith(".csv"):
            csv_to_use = csv
            
    try:
        clean_directory_path = ""
        raw_csv = pd.read_csv(f'{directory_path}{csv_to_use}')
        
    except:
        return
    
    oneHeader = testWallets(raw_csv, csv_to_use)
    
    if oneHeader == False:
        try:
            raw_csv = raw_csv.drop(['Txhash', 'Blockno','UnixTimestamp', 'DateTime','From','Token_ID','Value','Method'], axis=1)
            clean_csv = openSea(raw_csv)
            clean_csv.to_csv(f'{clean_directory_path}clean{csv_to_use}', index=False)  
            archive_directory_path = ""
            
            try:
                os.rename(f'{directory_path}{csv_to_use}',f'{archive_directory_path}{csv_to_use}')
                
            except:
                os.remove(f'{archive_directory_path}{csv_to_use}')
                os.rename(f'{directory_path}{csv_to_use}',f'{archive_directory_path}{csv_to_use}')
                
        except:
            raw_csv = raw_csv.drop(['Txhash', 'Blockno','UnixTimestamp', 'DateTime','From','Token_ID','Method'], axis=1)
            clean_csv = openSea(raw_csv)
            clean_csv.to_csv(f'{clean_directory_path}clean{csv_to_use}', index=False)  
            archive_directory_path = ""
            
            try:
                os.rename(f'{directory_path}{csv_to_use}',f'{archive_directory_path}{csv_to_use}')
                
            except:
                os.remove(f'{archive_directory_path}{csv_to_use}')
                os.rename(f'{directory_path}{csv_to_use}',f'{archive_directory_path}{csv_to_use}')
    else:
        return

while True:
    OpenSea_TwitterHandleScraping()
