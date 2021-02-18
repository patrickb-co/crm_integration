import requests
import json
import mysql.connector

instance_api_url_for_post_relationship = ""
instance_api_url = ""
module_name = "Accounts"
auth_url = instance_api_url + "access_token"
modules_url = instance_api_url + "V8/module/" + module_name 
post_url = instance_api_url + "V8/module"
client_id = ""
client_secret = ""
username = ""
password = ""
# cnpj_empresa_livre_c find in data set

def authenticateSuiteCRM(auth_url, client_id, client_secret, username, password):  
    payload = {"grant_type":"password","client_id":client_id,"client_secret":client_secret,"username":username,"password":password}
    auth_request = requests.post(auth_url,data = payload)
    crm_token = format(auth_request.json()["access_token"])
    
    return crm_token

print(authenticateSuiteCRM(auth_url,client_id,client_secret,username,password))
def getAccounts(url):
    request = requests.get(url, headers = {'Content-Type': 'application/json','Authorization': 'Bearer ' + authenticateSuiteCRM(auth_url,client_id,client_secret,username,password) })
    #return format(request.json())
    return request.json()

def postAccount(url,data_i): 
    request = requests.post(url, headers = {'Content-Type': 'application/json','Authorization': 'Bearer ' + authenticateSuiteCRM(auth_url,client_id,client_secret,username,password) }, data = data_i)
    return request.json()["data"]["id"]
    #return request.json()

def postContact(url,data_i):
    request = requests.post(url, headers = {'Content-Type': 'application/json','Authorization': 'Bearer ' + authenticateSuiteCRM(auth_url,client_id,client_secret,username,password) }, data = data_i)
    return request.json()["data"]["id"]
    #return request.json()
def postRelationship_canais_only(module_name_A, module_name_B, base_url, data_account_i, data_canais_i, account_id):
    #postAccount(post_url, data_account_i)
    url = base_url + "/Api/V8/module/" + module_name_A + "/"+account_id+"/relationships"

    payload = "{\"data\": {\"type\": \"" + module_name_B + "\",\"id\": \""+postContact(post_url, data_canais_i)+"\"}}"
    headers = {
    'Content-Type': 'application/json',
    'Authorization': 'Bearer ' + authenticateSuiteCRM(auth_url,client_id,client_secret,username,password) }

    response = requests.request("POST", url, headers=headers, data = payload)
    #print(response.text.encode('utf8'))   
    return response

def postRelationship_canais_accounts(module_name_A, module_name_B, base_url, data_account_i, data_canais_i):
    url = base_url + "/Api/V8/module/" + module_name_A + "/"+postAccount(post_url, data_account_i)+"/relationships"

    payload = "{\n  \"data\": {\n    \"type\": \"" + module_name_B + "\",\n    \"id\": \""+postContact(post_url, data_canais_i)+"\"\n  }\n}"
    headers = {
    'Content-Type': 'application/json',
    'Authorization': 'Bearer ' + authenticateSuiteCRM(auth_url,client_id,client_secret,username,password) }

    response = requests.request("POST", url, headers=headers, data = payload)
    print(response.text.encode('utf8'))   
    return response



#solicites database
#try/except 1
mydb = mysql.connector.connect(
  host="",
  user="",
  port=3306,
  password="",
  database=""
)
#print(mydb)
cursor = mydb.cursor(dictionary=True)

#DEFINE DATE RANGE:


cursor.execute("SELECT * FROM solicite_registros")# WHERE created_at >= DATE_ADD(NOW(), INTERVAL -1 DAY)") # last 24 hours considering cron

rows = cursor.fetchall()    

#end of try/except 1
cnpjs_crm =[]
ids_crm = []
#print(type(cnpjs_crm))
#try/except 2
for i in range(len(getAccounts(modules_url)['data'])):
    cnpjs_crm.append(getAccounts(modules_url)['data'][i]['attributes']['cnpj_empresa_livre_c']) #creates a list of all cnpjs in crm
    ids_crm.append(getAccounts(modules_url)['data'][i]['id'])
#end of try/except 2

for r in rows:
    
    if r["cnpj"] in cnpjs_crm: #compares cnpjs in solicites data set with cnpjs in crm
        print("THIS IS R[CNPJ]: "+r["cnpj"])
        print("THIS IS CRM CNPJ: "+cnpjs_crm[cnpjs_crm.index(r["cnpj"])])
        
        #update solicites base with crm uuid
        cursor.execute("UPDATE solicite_registros SET crm_uuid = '"+ ids_crm[cnpjs_crm.index(r['cnpj'])] +"' WHERE cnpj = '"+ r['cnpj']+"'")
        mydb.commit()

        #build json for Canais
        
        canal_data = '{"data": {"type": "TES12_CanaisTeste","attributes": {"valor_c":"'+str(r["valor"])+'","canal_data_c":"'+str(r["created_at"].strftime("%m/%d/%Y, %H:%M:%S"))+'","description":"'+json.dumps(r["json"]).strip('"')+'"}}}'

        #post somente de canais, + faz a relação com account, account já existe no crm
        
        print("This is the Response Status of postRelationship_canais_only: " + str(postRelationship_canais_only("Accounts","TES12_CanaisTeste",instance_api_url_for_post_relationship,"",canal_data,ids_crm[cnpjs_crm.index(r['cnpj'])])))

        
    else:
        
        #build json for accounts and Canais
        account_data = '{"data": {"type": "Accounts","attributes": {"name":"'+" "+'","cnpj_empresa_livre_c":"'+str(r["cnpj"])+'","municipio_empresa_c":" ","dada_constituicao_c":"20-10-1980","origem_prospeccao_c":" ","account_type":" ","contato_empresa_c":" ","contato_cargo_c":" ","phone_office":" ","phone_alternate":" ","email1":" "}}}'
       
        canal_data = '{"data": {"type": "TES12_CanaisTeste","attributes": {"valor_c":"'+str(r["valor"])+'","canal_data_c":"'+ str(r["created_at"].strftime("%m/%d/%Y, %H:%M:%S")) +'","description":"'+ json.dumps(r["json"]).strip('"') +'"}}}'
        
        #post Accounts, isola o id, posta o Canais using crm API
        #postAccount(post_url, account_data)
        this_account_id = str(postAccount(post_url, account_data))
        print(this_account_id)
        print("This is the Response Status of postRelationship_canais_only: " + str(postRelationship_canais_only("Accounts","TES12_CanaisTeste",instance_api_url_for_post_relationship,"",canal_data,this_account_id)))
        #update solicites base with crm uuid
        cursor.execute("UPDATE solicite_registros SET crm_uuid = '"+ this_account_id +"' WHERE cnpj = '"+ r['cnpj']+"'")
        mydb.commit()


mydb.close()
