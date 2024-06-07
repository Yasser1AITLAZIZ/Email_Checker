import pandas as pd
import numpy as np
import http.client
import json
import os
import time
from openpyxl import load_workbook


def process_files(directory):
    for filename in os.listdir(directory):
        if filename.endswith(".xlsx"):
            filepath = os.path.join(directory, filename)
            process_excel(filepath,directory)
            time.sleep(5) 

def process_excel(filepath,directory):
    # Charger le fichier Excel
    print(f"#############Processing {filepath}######################")
    data = pd.read_excel(filepath)
    data['formatCheck'] = '-'
    data['formatCheck'] = data['formatCheck'].astype('string')
    data['smtpCheck'] = '-'
    data['smtpCheck'] = data['smtpCheck'].astype('string')
    data['dnsCheck'] = '-'
    data['dnsCheck'] = data['dnsCheck'].astype('string')
    try : 
        emails = data['E‐Mail'].replace('None',np.nan).dropna().astype('string').tolist()
    except Exception as e:
        for col in data.columns:
            if "e-mail" in str(col).lower():
                email_col = str(col)
                break
        data.rename(columns={email_col:'E‐Mail'},inplace=True)
        emails = data['E‐Mail'].replace('None',np.nan).dropna().astype('string').tolist()

    # Configuration des headers pour la requête API
    headers = {
        'X-RapidAPI-Key': "",
        'X-RapidAPI-Host': "whoisapi-email-verification-v1.p.rapidapi.com"
    }

    # Traitement par batch
    batch_size = 8
    for i in range(0, len(emails), batch_size):
        print(f"Batch processed :{(i//8)+1}/{int(len(emails)//batch_size)+1}")
        # Créer une nouvelle connexion pour chaque batch
        conn = http.client.HTTPSConnection("whoisapi-email-verification-v1.p.rapidapi.com")
        batch = emails[i:i+batch_size]
        c=0
        for email in batch:
            c+=1
            print(f"emails processed :{c}/{len(batch)}")
            if str(email) in []:
                print(f"Desinscrit de l'emailing: {email}")
                update_excel(data, email, "Desinscrit de l'emailing",True)
                continue
            # Vérifier si l'email est vide ou non
            try :
                email.encode('ascii')
                safe_email = email.replace('@', '%40')
                try:
                    conn.request("GET", f"/api/v1?emailAddress={safe_email}&outputformat=JSON", headers=headers)
                    res = conn.getresponse()
                    response_data = json.loads(res.read().decode("utf-8"))
                    update_excel(data, email, response_data,False)
                except Exception as e:
                    print(f'------------------------------Erreur :{str(e)} with : {email}') 
                    update_excel(data, email, f"Erreur :{str(e)}",True)
                    time.sleep(4)
            except Exception as e:
                print(f'------------------------------Erreur encoding ascii with : {email}') 
                update_excel(data, email, "Erreur de codage ASCII dans le format",True)
        conn.close()  # Fermer la connexion après chaque batch
        time.sleep(5)  # Cooldown de 4 secondes
        
    data_verified = rechecking_error_emails(data)
    # Sauvegarder le fichier Excel modifié
    name_excel = filepath.split('/')[-1]
    storage_file = directory + f"/resultats/{name_excel}".replace(".xlsx","_checked_emails.xlsx") 
    data_verified.to_excel(storage_file, index=False)
    print(f"Processed file saved as {storage_file}")

def update_excel(data, email, response, index_):
    if index_ :
        index = data[data['E‐Mail'] == email].index[0]
        data.at[index, 'formatCheck'] = response
        data.at[index, 'smtpCheck'] = response
        data.at[index, 'dnsCheck'] = response
    else :
        index = data[data['E‐Mail'] == email].index[0]
        data.at[index, 'formatCheck'] = response.get("formatCheck")
        data.at[index, 'smtpCheck'] = response.get("smtpCheck")
        data.at[index, 'dnsCheck'] = response.get("dnsCheck")

def rechecking_error_emails(data):
    # Configuration des headers pour la requête API
    headers = {
        'X-RapidAPI-Key': "9d35cc5cb0msh1d1e06d36fcff3ap1a93c1jsnd755d1a26c47",
        'X-RapidAPI-Host': "whoisapi-email-verification-v1.p.rapidapi.com"
    }
    
    # Définir les valeurs acceptables
    acceptable_values = ["true", "Erreur de codage ASCII dans le format",'-',"Desinscrit de l'emailing"]
    # Filtrer pour trouver les lignes avec des valeurs non acceptables
    data_retesting = data[~data['smtpCheck'].isin(acceptable_values)]
    emails_retesting = data_retesting['E‐Mail'].astype('string').tolist()
    if len(emails_retesting)>0:
        print(f"*****Retesting false or error emails*****")
        # Traitement par batch
        if len(emails_retesting) >= 8:
            batch_size = 8
        else :
            batch_size = len(emails_retesting)
        for i in range(0, len(emails_retesting), batch_size):
            print(f"Batch retesting processed :{(i//8)+1}/{int(len(emails_retesting)//batch_size)+1}")
            # Créer une nouvelle connexion pour chaque batch
            conn = http.client.HTTPSConnection("whoisapi-email-verification-v1.p.rapidapi.com")
            batch = emails_retesting[i:i+batch_size]
            c_=0
            for email_ in batch:
                    c_+=1
                    print(f"emails retesting processed :{c_}/{len(batch)}")
                    try :  
                        email_.encode('ascii')
                        safe_email = email_.replace('@', '%40')
                        try:
                            conn.request("GET", f"/api/v1?emailAddress={safe_email}&outputformat=JSON", headers=headers)
                            res = conn.getresponse()
                            response_data = json.loads(res.read().decode("utf-8"))
                            update_excel(data, email_, response_data,False)
                        except Exception as e:
                            print(f'------------------------------Erreur :{str(e)} with : {email_}') 
                            update_excel(data, email_, f"Erreur :{str(e)}",True)
                            time.sleep(4)
                    except Exception as b :
                        print(f'------------------------------Erreur encoding ascii with : {email_}') 
                        update_excel(data, email_, f"Erreur de codage ASCII dans le format",True)
            conn.close()  # Fermer la connexion après chaque batch
            time.sleep(5)    
    return data 
    
    

directories = [
               "application_api_checker/A"]
for directory in directories:
    process_files(directory)
